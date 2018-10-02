/*
 * Copyright (C) 2017-2018 Dremio Corporation
 *
 *                         Licensed under the Apache License, Version 2.0 (the "License");
 *                         you may not use this file except in compliance with the License.
 *                         You may obtain a copy of the License at
 *
 *                         http://www.apache.org/licenses/LICENSE-2.0
 *
 *                         Unless required by applicable law or agreed to in writing, software
 *                         distributed under the License is distributed on an "AS IS" BASIS,
 *                         WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *                         See the License for the specific language governing permissions and
 *                         limitations under the License.
 */
package com.dremio.extras.plugins.kdb.rels;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.CustomPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.extras.plugins.kdb.KdbGroupScan;
import com.dremio.extras.plugins.kdb.rels.translate.KdbPrelVisitor;
import com.dremio.extras.plugins.kdb.rels.translate.KdbQueryParameters;
import com.google.common.base.Preconditions;

/**
 * Represents a finalized Kdb scan after a query has been generated. At this point, no further pushdowns
 * can be done. Contains the original KdbPrel tree so that we can continue to do operations like debug
 * inspection and RelMdOrigins determination. Beyond that, three tree should not be used. (For example,
 * it won't show up when doing EXPLAIN).
 */
public class KdbScanPrel extends AbstractRelNode implements KdbPrel, CustomPrel {

    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(KdbScanPrel.class);
    private final String sql;
    private final TableMetadata tableMetadata;
    private final List<SchemaPath> projectedColumns;
    private KdbPrel input;

    public KdbScanPrel(RelOptCluster cluster, RelTraitSet replace, KdbPrel input, TableMetadata tableMetadata, List<SchemaPath> projectedColumns, String sql) {
        super(cluster, replace);
        this.input = input;
        this.tableMetadata = tableMetadata;
        this.projectedColumns = projectedColumns;
        this.sql = sql;

    }

    @Override
    protected RelDataType deriveRowType() {
        return input.getRowType();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        Preconditions.checkArgument(inputs == null || inputs.size() == 0);
        return new KdbScanPrel(getCluster(), traitSet, input, tableMetadata, projectedColumns, sql);
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
        return creator.addMetadata(this, new KdbGroupScan(tableMetadata, projectedColumns, sql));
    }

    @Override
    public BatchSchema.SelectionVectorMode getEncoding() {
        return BatchSchema.SelectionVectorMode.DEFAULT[0];
    }

    @Override
    public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
        return logicalVisitor.visitPrel(this, value);
    }

    @Override
    public Iterator<Prel> iterator() {
        return PrelUtil.iter(getInputs());
    }

    @Override
    public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
        return BatchSchema.SelectionVectorMode.DEFAULT;
    }

    @Override
    public boolean needsFinalColumnReordering() {
        return true;
    }

    @Override
    public Prel getOriginPrel() {
        return input;
    }

    @Override
    public BatchSchema getSchema(FunctionLookupContext functionLookupContext) {
        return tableMetadata.getSchema();
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

}
