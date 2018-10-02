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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.extras.plugins.kdb.rels.translate.KdbPrelVisitor;
import com.dremio.extras.plugins.kdb.rels.translate.KdbQueryParameters;

/**
 * Implementation of a {@link Filter}
 * relational expression in MongoDB.
 */
public class KdbFilter extends FilterPrel implements KdbPrel {
    private final List<SchemaPath> projectedColumns;
    private final TableMetadata metadata;
    private final BatchSchema schema;

    public KdbFilter(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition, BatchSchema schema, List<SchemaPath> projectedColumns,
            TableMetadata metadata) {
        super(cluster, traitSet, child, condition);
        this.schema = schema;
        this.projectedColumns = projectedColumns;
        this.metadata = metadata;
//    assert getConvention() == KdbRel.CONVENTION;
        assert getConvention() == child.getConvention();

    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    public KdbFilter copy(RelTraitSet traitSet, RelNode input,
                          RexNode condition) {
        return new KdbFilter(getCluster(), traitSet, input, condition, schema, projectedColumns, metadata);
    }

    @Override
    public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
        throw new UnsupportedOperationException();
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
    public BatchSchema getSchema(FunctionLookupContext functionLookupContext) {
        return schema;
    }

    @Override
    public List<SchemaPath> projectedColumns() {
        return projectedColumns;
    }

    @Override
    public KdbQueryParameters accept(
            KdbPrelVisitor logicalVisitor, KdbQueryParameters value) {
        return logicalVisitor.visitFilter(this, value);
    }
}

// End KdbFilter.java
