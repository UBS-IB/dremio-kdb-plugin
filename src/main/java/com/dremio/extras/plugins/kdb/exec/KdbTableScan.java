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
package com.dremio.extras.plugins.kdb.exec;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.extras.plugins.kdb.rels.KdbRel;

/**
 * Relational expression representing a scan of a kdb table.
 */
public class KdbTableScan extends TableScan implements KdbRel {
    private final KdbTable kdbTable;
    private final RelDataType projectRowType;

    /**
     * Creates a KdbTableScan.
     *
     * @param cluster        Cluster
     * @param traitSet       Traits
     * @param table          Table
     * @param kdbTable       MongoDB table
     * @param projectRowType Fields and types to project; null to project raw row
     */
    protected KdbTableScan(RelOptCluster cluster, RelTraitSet traitSet,
                           RelOptTable table, KdbTable kdbTable, RelDataType projectRowType) {
        super(cluster, traitSet, table);
        this.kdbTable = kdbTable;
        this.projectRowType = projectRowType;

        assert kdbTable != null;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return this;
    }

    @Override
    public RelDataType deriveRowType() {
        return projectRowType != null ? projectRowType : super.deriveRowType();
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // scans with a small project list are cheaper
        final float f = projectRowType == null ? 1f
                : (float) projectRowType.getFieldCount() / 100f;
        return super.computeSelfCost(planner, mq).multiplyBy(.1 * f);
    }

    @Override
    public void register(RelOptPlanner planner) {

    }

}

// End KdbTableScan.java
