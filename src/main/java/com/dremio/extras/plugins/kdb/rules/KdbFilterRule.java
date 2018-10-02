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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;

/**
 * translate filter
 */
public final class KdbFilterRule extends RelOptRule {
    public static final KdbFilterRule INSTANCE = new KdbFilterRule();
    //private final RelTrait inTrait;

    private KdbFilterRule() {
//    super(LogicalFilter.class, Convention.NONE, Convention.NONE,//KdbRel.CONVENTION,
//      "KdbFilterRule");
        super(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(KdbIntermediatePrel.class)), "KdbFilterRule");
        //this.inTrait = (RelTrait)Preconditions.checkNotNull(in);
        //this.inTrait = (RelTrait)Preconditions.checkNotNull(out);
    }

    public KdbFilter convert(RelNode rel, KdbIntermediatePrel inter) {
        final FilterPrel filter = (FilterPrel) rel;
        final RelTraitSet traitSet = filter.getTraitSet();//.replace(out);

        return new KdbFilter(
                rel.getCluster(),
                traitSet,
                inter.getInput(),
                filter.getCondition(), inter.getSchema(inter.getFunctionLookupContext()), inter.projectedColumns(), inter.getTableMetadata());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        KdbIntermediatePrel interPrel = call.rel(1);
//    if (rel.getTraitSet().contains(this.inTrait)) {
        KdbFilter converted = this.convert(rel, interPrel);

//    KdbRel.Implementor implementor = new KdbRel.Implementor();
//      converted.implement(new KdbRel.Implementor());

        if (converted != null) {
            call.transformTo(interPrel.withNewInput(converted));

        }

    }
}
