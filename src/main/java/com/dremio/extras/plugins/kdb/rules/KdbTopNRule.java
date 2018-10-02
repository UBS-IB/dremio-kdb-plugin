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
package com.dremio.extras.plugins.kdb.rules;

import java.math.BigDecimal;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.TopNExposer;
import com.dremio.exec.planner.physical.TopNPrel;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;
import com.dremio.extras.plugins.kdb.rels.KdbLimit;
import com.google.common.base.Predicate;

/**
 * translate topn
 */
public final class KdbTopNRule extends RelOptRule {
    public static final KdbTopNRule INSTANCE = new KdbTopNRule();

    private KdbTopNRule() {
        super(RelOptHelper.some(TopNPrel.class, RelOptHelper.any(ExchangePrel.class, KdbIntermediatePrel.class)));
    }

    public boolean matches(RelOptRuleCall call) {
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final TopNPrel limitPrel = call.rel(0);
        final ExchangePrel ep = call.rel(1);
        final KdbIntermediatePrel intermediatePrel = call.rel(2);
        final RelTraitSet traitSet = limitPrel.getTraitSet().replace(TopNExposer.getCollation(limitPrel));
        RexBuilder builder = limitPrel.getCluster().getRexBuilder();
        KdbLimit newLimit = new KdbLimit(intermediatePrel.getCluster(), traitSet, intermediatePrel.getInput(),
//convert(intermediatePrel.getInput(), limitPrel.getTraitSet().replace(RelCollations.EMPTY)),
                builder.makeBigintLiteral(BigDecimal.ZERO),
                builder.makeBigintLiteral(BigDecimal.valueOf((long) TopNExposer.getLimit(limitPrel))),
                intermediatePrel.getSchema(intermediatePrel.getFunctionLookupContext()), intermediatePrel.projectedColumns());

        MoreRelOptUtil.NodeRemover er = new MoreRelOptUtil.NodeRemover(new Predicate<RelNode>() {
            @Override
            public boolean apply(@Nullable RelNode input) {
                assert input != null;
                return !ExchangePrel.class.isAssignableFrom(input.getClass());
            }
        });
        newLimit = (KdbLimit) newLimit.accept(er);
        final KdbIntermediatePrel newInter = intermediatePrel.withNewInput(newLimit);
        call.transformTo(newInter);
    }

}
