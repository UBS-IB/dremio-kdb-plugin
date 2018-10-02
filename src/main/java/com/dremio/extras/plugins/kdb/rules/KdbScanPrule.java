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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediateScanPrel;
import com.dremio.extras.plugins.kdb.rels.KdbScanDrel;

/**
 * Rule that converts kdb logical to physical scan
 */
public class KdbScanPrule extends ConverterRule {

    private final FunctionLookupContext lookupContext;

    public KdbScanPrule(FunctionLookupContext lookupContext) {
        super(KdbScanDrel.class, Rel.LOGICAL, Prel.PHYSICAL, "KdbScanPrule");
        this.lookupContext = lookupContext;
    }

    @Override
    public RelNode convert(RelNode rel) {
        KdbScanDrel logicalScan = (KdbScanDrel) rel;

        KdbIntermediateScanPrel physicalScan = new KdbIntermediateScanPrel(logicalScan.getCluster(), logicalScan.getTraitSet().replace(Prel.PHYSICAL),
                logicalScan.getTable(), logicalScan.getTableMetadata(),
                logicalScan.getProjectedColumns(),
                logicalScan.getObservedRowcountAdjustment());
//                                                                    logicalScan.getTableMetadata(),
//                                                                    logicalScan.getProjectedColumns(),
//                                                                    logicalScan.getObservedRowcountAdjustment());
        KdbIntermediatePrel converted = new KdbIntermediatePrel(
                logicalScan.getTraitSet().replace(Prel.PHYSICAL), physicalScan, lookupContext, physicalScan.getTableMetadata(),
                physicalScan.getProjectedColumns());
        //call.transformTo(converted);
        return converted;
    }

//  @Override
//  public void onMatch(RelOptRuleCall call) {
//
//  }
}
