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
package com.dremio.extras.plugins.kdb.exec;

import java.util.Collections;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.RecordReader;
import com.dremio.extras.plugins.kdb.KdbStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.sabot.op.scan.ScanOperator;
import com.dremio.sabot.op.spi.ProducerOperator;

/**
 * create single batches to run against kdb
 */
@SuppressWarnings("unused")
public class KdbScanBatchCreator implements ProducerOperator.Creator<KdbSubScan> {

    @Override
    public ProducerOperator create(FragmentExecutionContext fragmentExecContext, OperatorContext context, KdbSubScan subScan) throws ExecutionSetupException {
        KdbStoragePlugin plugin = fragmentExecContext.getStoragePlugin(subScan.getPluginId());

        KdbRecordReader innerReader = new KdbRecordReader(context, subScan.getColumns(), subScan.getSql(), plugin.getKdbSchema(), plugin.getBatchSize() == 0 ? subScan.getBatchSize() : plugin.getBatchSize(), subScan.getSchema());
        //CoercionReader reader = new CoercionReader(context, subScan.getColumns(), innerReader, subScan.getSchema());
        return new ScanOperator(fragmentExecContext.getSchemaUpdater(), subScan, context, Collections.<RecordReader>singletonList(innerReader).iterator());
    }
}
