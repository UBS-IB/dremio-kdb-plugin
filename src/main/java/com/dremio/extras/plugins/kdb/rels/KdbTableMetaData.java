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
package com.dremio.extras.plugins.kdb.rels;

import java.util.Iterator;
import java.util.List;

import com.dremio.datastore.SearchTypes;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitsKey;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.google.common.base.Predicate;

/**
 * wrapper around table metadata to insert new schema
 */
public class KdbTableMetaData implements TableMetadata {
    private final TableMetadata delegate;
    private final BatchSchema schema;

    public KdbTableMetaData(TableMetadata delegate, BatchSchema schema) {
        this.delegate = delegate;
        this.schema = schema;
    }

    @Override
    public NamespaceKey getName() {
        return delegate.getName();
    }

    @Override
    public StoragePluginId getStoragePluginId() {
        return delegate.getStoragePluginId();
    }

    @Override
    public FileConfig getFormatSettings() {
        return delegate.getFormatSettings();
    }

    @Override
    public ReadDefinition getReadDefinition() {
        return delegate.getReadDefinition();
    }

    @Override
    public DatasetType getType() {
        return delegate.getType();
    }

    @Override
    public String computeDigest() {
        return delegate.computeDigest();
    }

    @Override
    public String getUser() {
        return delegate.getUser();
    }

    @Override
    public TableMetadata prune(SearchTypes.SearchQuery partitionFilterQuery) throws NamespaceException {
        return delegate.prune(partitionFilterQuery);
    }

    @Override
    public TableMetadata prune(Predicate<DatasetSplit> splitPredicate) throws NamespaceException {
        return delegate.prune(splitPredicate);
    }

    @Override
    public TableMetadata prune(List<DatasetSplit> newSplits) throws NamespaceException {
        return delegate.prune(newSplits);
    }

    @Override
    public SplitsKey getSplitsKey() {
        return delegate.getSplitsKey();
    }

    @Override
    public Iterator<DatasetSplit> getSplits() {
        return delegate.getSplits();
    }

    @Override
    public double getSplitRatio() throws NamespaceException {
        return delegate.getSplitRatio();
    }

    @Override
    public int getSplitCount() {
        return delegate.getSplitCount();
    }

    @Override
    public BatchSchema getSchema() {
        return schema;
    }

    @Override
    public long getApproximateRecordCount() {
        return delegate.getApproximateRecordCount();
    }

    @Override
    public boolean isPruned() throws NamespaceException {
        return delegate.isPruned();
    }

    @Override
    public String getVersion() {
        return delegate.getVersion();
    }

    @Override
    public DatasetConfig getDatasetConfig() {
        return delegate.getDatasetConfig();
    }
}
