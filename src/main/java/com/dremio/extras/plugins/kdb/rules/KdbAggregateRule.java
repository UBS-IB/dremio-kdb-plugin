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

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.common.AggregateRelBase;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.AggPrelBase;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;

/**
 * translate aggregate
 */
public class KdbAggregateRule extends RelOptRule {
    private final FunctionLookupContext functionLookupContext;

    public KdbAggregateRule(FunctionLookupContext functionLookupContext) {
        super(RelOptHelper.some(AggregateRelBase.class, RelOptHelper.any(ExchangePrel.class, KdbIntermediatePrel.class), new RelOptRuleOperand[0]), "ElasticAggregateRule");
        this.functionLookupContext = functionLookupContext;
    }

    public void onMatch(RelOptRuleCall call) {
        AggPrelBase aggregate = (AggPrelBase) call.rel(0);
        KdbIntermediatePrel oldInter = (KdbIntermediatePrel) call.rel(2);
        KdbAggregate newAggregate = null;

        newAggregate = new KdbAggregate(
                oldInter.getInput().getCluster(),
                oldInter.getInput().getTraitSet(),
                oldInter.getInput(),
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                oldInter.getSchema(oldInter.getFunctionLookupContext()),
                oldInter.projectedColumns(),
                oldInter.getTableMetadata().getReadDefinition());

        KdbIntermediatePrel newInter = oldInter.withNewInput(newAggregate);
        call.transformTo(newInter);
    }

    public boolean matches(RelOptRuleCall call) {
        return true;
    }
}
