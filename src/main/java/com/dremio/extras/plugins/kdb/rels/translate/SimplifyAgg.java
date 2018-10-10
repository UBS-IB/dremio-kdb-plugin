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
package com.dremio.extras.plugins.kdb.rels.translate;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbProject;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * turn multiple agg/projects back into mean/std etc
 */
public class SimplifyAgg {
    private KdbQueryParameters parameters;
    private RelDataTypeFactory typeFactory;
    private ReadDefinition readDefinition;
    private FunctionLookupContext functionLookupContext;
    private RelOptCluster cluster;

    public SimplifyAgg(
            KdbQueryParameters parameters, RelDataTypeFactory typeFactory, ReadDefinition readDefinition, FunctionLookupContext functionLookupContext,
            RelOptCluster cluster) {
        this.parameters = parameters;
        this.typeFactory = typeFactory;
        this.readDefinition = readDefinition;
        this.functionLookupContext = functionLookupContext;
        this.cluster = cluster;
    }

    private Pair<KdbAggregate, KdbProject> simplifyProjGroupby() {
        List<KdbProject> projects = Lists.newArrayList();
        List<KdbAggregate> aggs = Lists.newArrayList();
        if (parameters.getProject() != null) {
            projects.add(parameters.getProject());
        }
        if (parameters.getAggregate() != null) {
            aggs.add(parameters.getAggregate());
        } else {
            KdbQueryParameters.KdbPrelContainer scan = parameters.getScan();
            if (scan.getQuery() != null && scan.getQuery().getAggregate() != null) {
                aggs.add(scan.getQuery().getAggregate());
            }

        }
        //todo handle multiple aggs/projects
        if (projects.size() > 1 || aggs.size() > 1) {
            throw new UnsupportedOperationException();
        }
        if (projects.isEmpty() || aggs.isEmpty()) {
            //todo not simplifying yet...
            return null;
        }
        KdbProject project = projects.get(0);
        KdbAggregate agg = aggs.get(0);
        List<RexNode> projections = project.getProjects();
        KdbAggregate newAgg = null;
        KdbProject newProj = null;
        for (RexNode r : projections) {
            newAgg = checkIsAvg(r, project, agg);
            if (newAgg != null) {
                BatchSchema newSchema = project.getSchema(functionLookupContext)
                        .maskAndReorder(
                                project.getRowType().getFieldList().stream().map(f -> SchemaPath.getSimplePath(f.getName().replace("xx_xx", "$"))).collect(Collectors.toList()));
                List<RexNode> newProjections = Lists.newArrayList();
                newProjections.addAll(projections);
                int index = newProjections.indexOf(r);
                if (index > -1) {
                    newProjections.remove(index);
                    newProjections.add(index, new RexInputRef(index, typeFactory.copyType(r.getType())));
                }
                newProj = new KdbProject(
                        project.getCluster(),
                        project.getTraitSet(),
                        newAgg,
                        newProjections,
                        project.getRowType(),
                        newSchema,
                        project.projectedColumns()
                );


                break;
            }
        }
        return Pair.of(newAgg, newProj);
    }

    private KdbAggregate checkIsAvg(RexNode r, KdbProject project, KdbAggregate aggregate) {
        String s = r.accept(new RexVisitor(typeFactory, TranslateProject.kdbFieldNames(project.getInput().getRowType())));
        String[] args = (s == null) ? new String[0] : s.split(";");
        if (args.length != 2) {
            return null;
        }

        RelDataTypeField f1 = aggregate.getRowType().getField(args[0].replace("`", ""), true, true);
        RelDataTypeField f2 = aggregate.getRowType().getField(args[1].replace("`", ""), true, true);
        Set<RelDataTypeField> aggs = Sets.newHashSet(f1, f2);
        Set<SqlKind> types = Sets.newHashSet(SqlKind.SUM0, SqlKind.COUNT);
        int count = aggregate.getGroupSet().length();
        List<Integer> argList = null;
        for (AggregateCall agg : aggregate.getAggCallList()) {
            SqlKind kind = agg.getAggregation().kind;
            if (kind.equals(SqlKind.SUM0) || kind.equals(SqlKind.COUNT) || kind.equals(SqlKind.SUM)) {
                RelDataTypeField field = aggregate.getRowType().getFieldList().get(count++);
                if (!aggs.remove(field)) {
                    return null;
                }
                if (!types.remove(kind)) {
                    return null;
                }
                if (argList == null) {
                    argList = agg.getArgList();
                } else {
                    if (!argList.equals(agg.getArgList())) {
                        return null;
                    }
                }
            }
        }
        if (types.isEmpty() && aggs.isEmpty()) {
            AggregateCall newAggCall = AggregateCall.create(
                    SqlStdOperatorTable.AVG, false, argList, -1, typeFactory.copyType(r.getType()), null);
            KdbAggregate newAgg = new KdbAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet(),
                    aggregate.getInput(),
                    aggregate.indicator,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    ImmutableList.of(newAggCall),
                    project.getSchema(functionLookupContext),
                    project.projectedColumns(),
                    readDefinition
            );

            return newAgg;
        }
        return null;
    }

    public KdbQueryParameters go() {
        Pair<KdbAggregate, KdbProject> agg = simplifyProjGroupby();
        if (agg == null || agg.right == null) {
            return parameters;
        }
        KdbQueryParameters queryParameters = new KdbQueryParameters();
        queryParameters.setAggregate(agg.left);
        queryParameters.setProject(agg.right);
        queryParameters.setLimit(parameters.getLimit());
        queryParameters.setSort(parameters.getSort());
        queryParameters.setFilter(parameters.getFilter());
        queryParameters.setScan(parameters.getScan());
        return queryParameters;
    }

    private static class RexVisitor extends RexVisitorImpl<String> {

        private final List<String> inFields;
        private RelDataTypeFactory typeFactory;

        protected RexVisitor(RelDataTypeFactory typeFactory, List<String> inFields) {
            super(true);
            this.typeFactory = typeFactory;
            this.inFields = inFields;
        }

        @Override
        public String visitInputRef(RexInputRef inputRef) {
            return "`" + inFields.get(inputRef.getIndex());//maybeQuote(
            //"$" + inFields.get(inputRef.getIndex()));
        }

        public List<String> visitList(List<RexNode> list) {
            final List<String> strings = new ArrayList<String>();
            for (RexNode node : list) {
                strings.add(node.accept(this));
            }
            return strings;
        }

        @Override
        public String visitCall(RexCall call) {

            if (call.op.equals(SqlStdOperatorTable.DIVIDE)) {
                List<RexNode> nodes = call.getOperands();
                List<String> operands = visitList(nodes);
                return Joiner.on(";").join(operands.stream().map(s -> s.replace("xx_xx", "$")).iterator());
            } else if (call.op.equals(SqlStdOperatorTable.CASE)) {
                List<RexNode> nodes = call.getOperands();
                List<String> operands = visitList(nodes);
                operands.remove(0);
                return Joiner.on(";").join(operands.stream().filter(Objects::nonNull).iterator());
            } else if (call.getKind() == SqlKind.CAST) {
                List<RexNode> nodes = call.getOperands();
                List<String> operands = visitList(nodes);
                return operands.get(0);
            }
            return super.visitCall(call);
        }

    }


}
