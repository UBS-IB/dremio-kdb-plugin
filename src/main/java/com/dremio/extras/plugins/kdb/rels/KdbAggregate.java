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
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.AggregateRelBase;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.extras.plugins.kdb.rels.translate.KdbPrelVisitor;
import com.dremio.extras.plugins.kdb.rels.translate.KdbQueryParameters;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;

/**
 * Implementation of
 * {@link org.apache.calcite.rel.core.Aggregate} relational expression
 * in MongoDB.
 */
public class KdbAggregate extends AggregateRelBase implements KdbPrel, KdbTerminalPrel {
    private final BatchSchema schema;
    private final List<SchemaPath> projectedColumns;
    private final ReadDefinition readDefinition;
    private final double version;

    public KdbAggregate(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode child, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls, BatchSchema schema, List<SchemaPath> schemaPaths, ReadDefinition readDefinition) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
        this.schema = schema;
        this.projectedColumns = schemaPaths;
        this.readDefinition = readDefinition;
        assert getConvention() == child.getConvention();
        String versionStr = readDefinition.getExtendedProperty().toStringUtf8();
        version = Double.parseDouble(versionStr.split(":")[1].replace("}", ""));
    }


    @Override
    public Aggregate copy(
            RelTraitSet traitSet, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {

        return new KdbAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls,
                schema, projectedColumns, readDefinition);
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
        final KdbPrel child = (KdbPrel) getInput().accept(new MoreRelOptUtil.SubsetRemover());
        final BatchSchema childSchema = child.getSchema(functionLookupContext);
        BatchSchema xxx = childSchema.merge(ExpressionTreeMaterializer.materializeFields(getAggExpressions(), childSchema, functionLookupContext)
                .setSelectionVectorMode(childSchema.getSelectionVectorMode())
                .build());
        return xxx;
    }

    protected List<NamedExpression> getAggExpressions() {
        return RexToExpr.aggsToExpr(getRowType(), input, groupSet, aggCalls);
    }

    @Override
    public List<SchemaPath> projectedColumns() {
        return projectedColumns;
    }

    @Override
    public KdbQueryParameters accept(
            KdbPrelVisitor logicalVisitor, KdbQueryParameters value) {
        return logicalVisitor.visitAggregate(this, value);
    }

}

// End KdbAggregate.java
