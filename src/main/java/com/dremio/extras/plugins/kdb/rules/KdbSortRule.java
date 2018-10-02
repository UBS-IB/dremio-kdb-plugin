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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.SortPrel;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;
import com.dremio.extras.plugins.kdb.rels.KdbSort;

/**
 * translate sort
 */
public final class KdbSortRule<S extends Sort> extends RelOptRule {
    public static final KdbSortRule PHYSICAL_INSTANCE = new KdbSortRule(SortPrel.class, Prel.PHYSICAL, "MongoSortPhysicalPrule");

    private KdbSortRule(Class<S> sortPrelClass, Convention physical, String mongoSortPhysicalPrule) {
        super(RelOptHelper.some(sortPrelClass, RelOptHelper.any(ExchangePrel.class, KdbIntermediatePrel.class)), mongoSortPhysicalPrule);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        KdbIntermediatePrel oldInter = (KdbIntermediatePrel) call.rel(2);
        return !oldInter.isHasTerminalPrel();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        KdbIntermediatePrel interPrel = call.rel(2);
        ExchangePrel ep = call.rel(1);


//    MoreRelOptUtil.NodeRemover nr = new MoreRelOptUtil.NodeRemover(new Predicate<RelNode>() {
//      @Override
//      public boolean apply(@Nullable RelNode input) {
//        return !(input instanceof ExchangePrel);
//      }
//    });
//    rel = rel.accept(new MoreRelOptUtil.SubsetRemover()).accept(nr);
//    rel = RelOptUtil.replace(rel, ep, interPrel);
        KdbSort converted = this.convert(rel, interPrel);

        if (converted != null) {
            call.transformTo(interPrel.withNewInput(converted));//.accept(new AllExchangeRemover(), null));

        }

    }

    public KdbSort convert(RelNode rel, KdbIntermediatePrel inter) {
        final Sort sort = (Sort) rel;
        final RelTraitSet traitSet =
                sort.getTraitSet().replace(sort.getCollation());//.replace(Prel.PHYSICAL)

        return new KdbSort(rel.getCluster(), traitSet,
                //convert(sort.getInput(), traitSet.replace(RelCollations.EMPTY)),
                inter.getInput(),
                sort.getCollation(), sort.offset, sort.fetch, inter.getSchema(inter.getFunctionLookupContext()), inter.projectedColumns());
    }


}
