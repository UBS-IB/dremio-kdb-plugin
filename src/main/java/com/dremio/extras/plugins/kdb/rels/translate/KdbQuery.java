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
package com.dremio.extras.plugins.kdb.rels.translate;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;

import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbLimit;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.KdbProject;
import com.dremio.extras.plugins.kdb.rels.KdbSort;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.collect.ImmutableList;

/**
 * concrete implementation of visitor
 */
public class KdbQuery implements KdbPrelVisitor {
    private final ReadDefinition readDefinition;
    private final BatchSchema schema;

    public KdbQuery(ReadDefinition readDefinition, BatchSchema schema) {
        this.readDefinition = readDefinition;
        this.schema = schema;
    }


    @Override
    public KdbQueryParameters visitExchange(ExchangePrel prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        return value;
    }

    @Override
    public KdbQueryParameters visitScreen(ScreenPrel prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        return value;
    }

    @Override
    public KdbQueryParameters visitWriter(WriterPrel prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        return value;
    }

    @Override
    public KdbQueryParameters visitLeaf(LeafPrel prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        return value;
    }

    @Override
    public KdbQueryParameters visitJoin(JoinPrel prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        return value;
    }

    @Override
    public KdbQueryParameters visitProject(KdbProject prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        value.setProject(prel);
        return value;
    }

    @Override
    public KdbQueryParameters visitAggregate(KdbAggregate prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        if (value.getAggregate() == null) {
            value.setAggregate(prel);
            return value;
        } else {
            KdbAggregate distinct = isDistinct(prel, value.getAggregate());
            if (distinct != null) {
                value.setAggregate(distinct);
                return value;
            }
            KdbQueryParameters params = new KdbQueryParameters();
            params.setScan(value);
            params.setAggregate(prel);
            return params;
        }
    }

    @Override
    public KdbQueryParameters visitLimit(KdbLimit prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        value.setLimit(prel);
        return value;
    }

    @Override
    public KdbQueryParameters visitSort(KdbSort prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        value.setSort(prel);
        return value;
    }

    @Override
    public KdbQueryParameters visitFilter(KdbFilter prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);
        value.setFilter(prel);
        return value;
    }

    private KdbQueryParameters visitChildren(Prel prel, KdbQueryParameters value) {
        for (RelNode n : prel.getInputs()) {
            if (n instanceof KdbPrel) {
                value = ((KdbPrel) n).accept(this, value);
            } else if (n instanceof RelSubset) {
                RelNode best = ((RelSubset) n).getBest();
                assert best instanceof KdbPrel;
                value = ((KdbPrel) best).accept(this, value);
            }
        }
        return value;
    }

    @Override
    public KdbQueryParameters visitPrel(Prel prel, KdbQueryParameters value) {
        value = visitChildren(prel, value);

        if (value.scan() == null) {
            value.setScan(prel);
        } else {
            KdbQueryParameters params = new KdbQueryParameters();
            params.setScan(value);
            return params;
        }

        return value;
    }


    private KdbAggregate isDistinct(KdbAggregate aggregate, KdbAggregate oldAgg) {
        KdbAggregate newAggregate = null;
        if (oldAgg.groupSets.size() >= 1 && oldAgg.getAggCallList().isEmpty()) {
            //collapsible?
            if (aggregate.getAggCallList().size() == 1) {
                AggregateCall aggCall = aggregate.getAggCallList().get(0);
                if (aggCall.getAggregation().kind == SqlKind.COUNT) {
                    //probably a distinct count originally...lets try and fix
                    AggregateCall newAggCall = AggregateCall.create(
                            aggCall.getAggregation(), true, aggCall.getArgList(), -1, aggCall.getType(), aggCall.getName());


                    newAggregate = new KdbAggregate(oldAgg.getCluster(), oldAgg.getTraitSet(), oldAgg.getInput(), aggregate.indicator,
                            aggregate.getGroupSet(), aggregate.getGroupSets(), ImmutableList.of(newAggCall), schema,
                            oldAgg.projectedColumns(), readDefinition);

                }
            }
        }
        return newAggregate;
    }
}
