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
package com.dremio.extras.plugins.kdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.extras.plugins.kdb.exec.KdbSchema;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.protostuff.ByteString;

/**
 * plugin definition for kdb source
 */
public class KdbStoragePlugin implements StoragePlugin {
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbStoragePlugin.class);
    private final SabotContext context;
    private final String name;
    private final KdbSchema calciteConnector;
    private final Map<String, SourceTableDefinition> setMap = Maps.newHashMap();
    private final int batchSize;
    private boolean built = false;
    private ArrayList<SourceTableDefinition> dataSets;

    public KdbStoragePlugin(KdbStoragePluginConfig kdbConfig, SabotContext context, String name) {
        this.context = context;
        this.name = name;
        int x = 0;
        try {
            x = kdbConfig.fetchSize;
        } catch (Throwable t) {
            x = 0;
        }
        this.batchSize = x;
        calciteConnector = new KdbSchema(
                kdbConfig.host, kdbConfig.port, kdbConfig.username, kdbConfig.password);
    }

    private void buildDataSets() {
        if (!built) {
            dataSets = Lists.newArrayList();
            for (String table : calciteConnector.getTableNames()) {
                KdbTableDefinition def = new KdbTableDefinition(name, table, calciteConnector);
                dataSets.add(def);
                setMap.put(table, def);
            }
            built = true;
        }
    }

    @Override
    public SourceState getState() {
        try {
            calciteConnector.getTableNames();
            return SourceState.GOOD;
        } catch (Exception t) {
            return SourceState.badState(t);
        }
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
        return new SourceCapabilities(new BooleanCapabilityValue(SourceCapabilities.SUPPORTS_CONTAINS, true),
                new BooleanCapabilityValue(SourceCapabilities.SUBQUERY_PUSHDOWNABLE, true) //,
//      new BooleanCapabilityValue(SourceCapabilities.TREAT_CALCITE_SCAN_COST_AS_INFINITE, true)
        );
    }


    @Override
    public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
        return null;
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
        return context.getConfig().getClass("com.dremio.extras.plugins.kdb", StoragePluginRulesFactory.class, KdbRulesFactory.class);
    }

    @Override
    public CheckResult checkReadSignature(ByteString byteString, DatasetConfig datasetConfig, DatasetRetrievalOptions datasetRetrievalOptions) throws Exception {
        NamespaceKey namespaceKey = new NamespaceKey(datasetConfig.getFullPathList());
        final SourceTableDefinition definition = getDataset(namespaceKey, datasetConfig, datasetRetrievalOptions);

        if (definition == null) {
            return CheckResult.DELETED;
        }

        return new CheckResult() {

            @Override
            public UpdateStatus getStatus() {
                return UpdateStatus.CHANGED;
            }

            @Override
            public SourceTableDefinition getDataset() {
                return definition;
            }
        };
    }

    @Override
    public void start() throws IOException {

    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
        return true; //todo...everyone has access if the kdb table exists
    }

    @Override
    public boolean datasetExists(NamespaceKey key) {
        return false;
    }

    @Override
    public Iterable<SourceTableDefinition> getDatasets(String s, DatasetRetrievalOptions datasetRetrievalOptions) throws Exception {
        buildDataSets();
        return dataSets;
    }

    @Override
    public SourceTableDefinition getDataset(NamespaceKey namespaceKey, DatasetConfig datasetConfig, DatasetRetrievalOptions datasetRetrievalOptions) throws Exception {
        buildDataSets();
        if (namespaceKey.getPathComponents().size() != 2) {
            return null;
        }
        return setMap.get(namespaceKey.getPathComponents().get(1));
    }

    @Override
    public boolean containerExists(NamespaceKey key) {
        if (key.size() != 2) {
            return false;
        }
        try {
            calciteConnector.getTableNames();
            return key.getPathComponents().get(1).equals(name);
        } catch (Throwable e) {
            return false;
        }
    }

    public KdbSchema getKdbSchema() {
        return calciteConnector;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("close kdb source");
    }

}
