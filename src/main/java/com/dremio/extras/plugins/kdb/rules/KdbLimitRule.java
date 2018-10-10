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
import org.apache.calcite.rex.RexLiteral;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.LimitPrel;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;
import com.dremio.extras.plugins.kdb.rels.KdbLimit;

/**
 * translate limit
 */
public class KdbLimitRule extends RelOptRule {

    public static final KdbLimitRule INSTANCE = new KdbLimitRule();

    public KdbLimitRule() {
        super(RelOptHelper.some(LimitPrel.class, RelOptHelper.any(ExchangePrel.class, KdbIntermediatePrel.class)), "KdbLimitRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LimitPrel limit = call.rel(0);
        final KdbIntermediatePrel intermediatePrel = call.rel(2);

        // TODO this can probably be supported in many cases.
        if (limit.getOffset() != null && RexLiteral.intValue(limit.getOffset()) != 0) {
            return false;
        }

        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LimitPrel limitPrel = call.rel(0);
        final KdbIntermediatePrel intermediatePrel = call.rel(2);

        KdbLimit newLimit = new KdbLimit(limitPrel.getCluster(), limitPrel.getTraitSet(),
                intermediatePrel.getInput(),//convert(limitPrel.getInput(), limitPrel.getTraitSet().replace(RelCollations.EMPTY)),
                limitPrel.getOffset(), limitPrel.getFetch(), intermediatePrel.getSchema(intermediatePrel.getFunctionLookupContext()), intermediatePrel.projectedColumns());


        final KdbIntermediatePrel newInter = intermediatePrel.withNewInput(newLimit);
        call.transformTo(newInter);

    }
}
