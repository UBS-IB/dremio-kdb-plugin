/*
 * Copyright (C) 2017-2019 UBS Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.extras.plugins.kdb.rels;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlExplainLevel;
import org.codehaus.jackson.map.ObjectMapper;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.SinglePrel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.planner.sql.handlers.PrelFinalizable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.extras.plugins.kdb.KdbStoragePluginConfig;
import com.dremio.extras.plugins.kdb.rels.translate.KdbPrelVisitor;
import com.dremio.extras.plugins.kdb.rels.translate.KdbQueryGenerator;
import com.dremio.extras.plugins.kdb.rels.translate.KdbQueryParameters;
import com.dremio.service.Pointer;
import com.google.common.base.Preconditions;


/**
 * A boundary Prel. Is used by Elastic rules to ensure that only the right set
 * of operations are pushed down into Elastic. Maintains state of what is pushed down.
 * All Elastic operations are pushed down using an operand of
 * <PrelToFind><ElasticsearchIntermediatePrel>. This ensures that no matching/changes
 * are done below the intermediate prel.
 * <p>
 * This prel exists until we complete planning and is then finalized into a
 * ElasticFinalPrel which is a leaf prel for parallelization and execution purposes
 */
public class KdbIntermediatePrel extends SinglePrel implements PrelFinalizable, KdbPrel {

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(KdbIntermediatePrel.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final FunctionLookupContext functionLookupContext;
    private final TableMetadata tableMetadata;
    private final List<SchemaPath> projectedColumns;
    private final boolean hasTerminalPrel;

    public KdbIntermediatePrel(
            RelTraitSet traitSet,
            RelNode input,
            FunctionLookupContext functionLookupContext, TableMetadata tableMetadata, List<SchemaPath> projectedColumns) {
        super(input.getCluster(), traitSet, input);
        this.tableMetadata = tableMetadata;
        this.projectedColumns = projectedColumns;
        this.input = input;
        this.functionLookupContext = functionLookupContext;
        final Pointer<Boolean> hasTerminalPrelPointer = new Pointer<Boolean>(false);
        input.accept(new MoreRelOptUtil.SubsetRemover(false)).childrenAccept(new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (KdbTerminalPrel.class.isAssignableFrom(node.getClass())) {
                    hasTerminalPrelPointer.value = true;
                }
                super.visit(node, ordinal, parent);
            }
        });
        this.hasTerminalPrel = hasTerminalPrelPointer.value || (KdbTerminalPrel.class.isAssignableFrom(input.getClass()));
    }

    public boolean isHasTerminalPrel() {
        return hasTerminalPrel;
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException("Must be finalized before retrieving physical operator.");
    }


    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs.size() == 1, "must have one input: %s", inputs);
        return new KdbIntermediatePrel(traitSet, inputs.get(0), functionLookupContext, tableMetadata, projectedColumns);
    }

    public KdbIntermediatePrel withNewInput(Prel input) {
        return new KdbIntermediatePrel(input.getTraitSet(), input, functionLookupContext, tableMetadata, projectedColumns);
    }

    public KdbIntermediatePrel filter(final Predicate<RelNode> predicate) {
        return withNewInput((Prel) getInput().accept(new MoreRelOptUtil.SubsetRemover()).accept(new MoreRelOptUtil.NodeRemover(predicate)));
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return copy(getTraitSet(), getInputs());
    }

    @Override
    public BatchSchema.SelectionVectorMode getEncoding() {
        return BatchSchema.SelectionVectorMode.NONE;
    }

    @Override
    public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
        throw new UnsupportedOperationException("This needs to be finalized before using a PrelVisitor.");
    }

    @Override
    public boolean needsFinalColumnReordering() {
        return false;
    }

    /**
     * Finalize the stack. We'll do the following:
     * <p>
     * - Push limit and sample below project (if either exists)
     * - Pop a project out of the stack if it is an edge project.
     * - Add an external Limit to the lesser of sample or limit.
     * - Add the popped project as a ProjectPrel outside the pushdown.
     */
    @Override
    public Prel finalizeRel() {
        KdbPrel prel = (KdbPrel) input;
        return get(prel);
    }

    private KdbScanPrel get(KdbPrel prel) {
        KdbQueryGenerator queryGenerator = new KdbQueryGenerator(prel, this.projectedColumns, this.tableMetadata, functionLookupContext, getCluster(), getSchema(functionLookupContext));
        String query = queryGenerator.generate();
        logQuery(prel, query);
        return new KdbScanPrel(getCluster(), getTraitSet(), prel, queryGenerator.tableMetadata(), queryGenerator.projectedColumns(), query);
    }

    private void logQuery(KdbPrel prel, String query) {
        String sql = "unknown";
        try {
            RelToSqlConverter converter = new KdbRelToSqlConverter(SqlDialect.CALCITE);
            SqlImplementor.Result sqlResult = converter.visit(prel);
            sql = sqlResult.asSelect().toString();
        } catch (Throwable t) {
            LOGGER.warn("Couldn't translate query ", t);
        }
        KdbStoragePluginConfig connectionConfig = tableMetadata.getStoragePluginId().getConnectionConf();
        LOGGER.info("Plan was " + RelOptUtil.toString(input, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        try {
            LOGGER.info("kdb.query.logger - " + MAPPER.writeValueAsString(new QueryLog(query, sql, connectionConfig)));
        } catch (Throwable t) {
            //no-op
        }
    }

    @Override
    public BatchSchema getSchema(FunctionLookupContext functionLookupContext) {
        try {
            return ((KdbPrel) input).getSchema(functionLookupContext);
        } catch (ClassCastException e) {
            try {
                return ((KdbPrel) ((RelSubset) input).getBest()).getSchema(functionLookupContext);
            } catch (Throwable t) {
                return tableMetadata.getSchema();
            }
        }
    }

    public FunctionLookupContext getFunctionLookupContext() {
        return functionLookupContext;
    }

    @Override
    public List<SchemaPath> projectedColumns() {
        return projectedColumns;
    }

    @Override
    public KdbQueryParameters accept(
            KdbPrelVisitor logicalVisitor, KdbQueryParameters value) {
        return logicalVisitor.visitPrel(this, value);
    }

    public TableMetadata getTableMetadata() {
        return tableMetadata;
    }

    private static final class QueryLog {
        private final String kdbQuery;
        private final String sqlQuery;
        private final String connection;

        private QueryLog(String kdbQuery, String sqlQuery, KdbStoragePluginConfig config) {
            this.kdbQuery = kdbQuery;
            this.sqlQuery = sqlQuery;
            String userPass = (config.username == null || config.username.isEmpty()) ? "" : config.username + ":***********@";
            this.connection = userPass + config.host + ":" + config.port;
        }

        public String getKdbQuery() {
            return kdbQuery;
        }

        public String getSqlQuery() {
            return sqlQuery;
        }

        public String getConnection() {
            return connection;
        }
    }
}
