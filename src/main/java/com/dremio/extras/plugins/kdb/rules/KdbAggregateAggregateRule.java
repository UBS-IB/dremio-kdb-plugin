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
package com.dremio.extras.plugins.kdb.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.AggPrelBase;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.google.common.collect.ImmutableList;

/**
 * collapse two aggregates
 */
public class KdbAggregateAggregateRule extends RelOptRule {
    private final FunctionLookupContext functionLookupContext;

    public KdbAggregateAggregateRule(FunctionLookupContext functionLookupContext) {
        super(RelOptHelper.some(AggPrelBase.class, RelOptHelper.any(KdbPrel.class, KdbAggregate.class), new RelOptRuleOperand[0]), "KdbAggregateFlattenRule");
        this.functionLookupContext = functionLookupContext;
    }

    public void onMatch(RelOptRuleCall call) {
        AggPrelBase aggregate = (AggPrelBase) call.rel(0);
        KdbIntermediatePrel oldInter = (KdbIntermediatePrel) call.rel(1);
        KdbAggregate oldAgg = (KdbAggregate) call.rel(2);
        KdbAggregate newAggregate = null;

        if (oldAgg.groupSets.size() >= 1 && oldAgg.getAggCallList().isEmpty()) {
            //collapsible?
            if (aggregate.getAggCallList().size() == 1) {
                AggregateCall aggCall = aggregate.getAggCallList().get(0);
                if (aggCall.getAggregation().kind == SqlKind.COUNT) {
                    //probably a distinct count originally...lets try and fix
                    AggregateCall newAggCall = AggregateCall.create(aggCall.getAggregation(), true, aggCall.getArgList(), -1, aggCall.getType(), aggCall.getName());

                    newAggregate = new KdbAggregate(
                            oldAgg.getInput().getCluster(),
                            oldAgg.getInput().getTraitSet(),
                            oldAgg.getInput(),
                            aggregate.indicator,
                            aggregate.getGroupSet(),
                            aggregate.getGroupSets(),
                            ImmutableList.of(newAggCall),
                            oldAgg.getSchema(functionLookupContext),
                            oldAgg.projectedColumns(),
                            oldInter.getTableMetadata().getReadDefinition());
                }
            }
        }
        KdbIntermediatePrel newInter = oldInter.filter(input -> !(input instanceof KdbAggregate)).withNewInput(newAggregate);
        call.transformTo(newInter);


    }

    public boolean matches(RelOptRuleCall call) {
//    AggPrelBase aggregate = (AggPrelBase)call.rel(0);
//    KdbIntermediatePrel oldInter = (KdbIntermediatePrel)call.rel(1);
//    return !oldInter.isHasTerminalPrel();
        return true;
    }
}
