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
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.extras.plugins.kdb.rels.translate.KdbPrelVisitor;
import com.dremio.extras.plugins.kdb.rels.translate.KdbQueryParameters;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Project}
 * relational expression in kdb.
 */
public class KdbProject extends ProjectPrel implements KdbRel, KdbPrel {
    private final List<SchemaPath> projectedColumns;
    private final BatchSchema tableMetadata;


    public KdbProject(RelOptCluster cluster, RelTraitSet traitSet,
                      RelNode input, List<RexNode> projects, RelDataType rowType, BatchSchema tableMetadata, List<SchemaPath> projectedColumns) {
        super(cluster, traitSet, input, projects, rowType);
        this.tableMetadata = tableMetadata;
        this.projectedColumns = projectedColumns;
//    assert getConvention() == KdbRel.CONVENTION;
        assert getConvention() == input.getConvention();
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                        List<RexNode> projects, RelDataType rowType) {
        return new KdbProject(getCluster(), traitSet, input, projects,
                rowType, tableMetadata, projectedColumns);
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
    public List<SchemaPath> projectedColumns() {
        return projectedColumns;
    }

    @Override
    public KdbQueryParameters accept(
            KdbPrelVisitor logicalVisitor, KdbQueryParameters value) {
        return logicalVisitor.visitProject(this, value);
    }


    @Override
    public BatchSchema getSchema(FunctionLookupContext context) {
        final KdbPrel child = (KdbPrel) getInput().accept(new MoreRelOptUtil.SubsetRemover());
        final BatchSchema childSchema = child.getSchema(context);
        ParseContext parseContext = new ParseContext(PrelUtil.getSettings(getCluster()));
        return childSchema.merge(ExpressionTreeMaterializer.materializeFields(getProjectExpressions(parseContext), childSchema, context)
                .setSelectionVectorMode(childSchema.getSelectionVectorMode())
                .build());
    }

}

// End KdbProject.java
