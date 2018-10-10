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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.codehaus.jackson.map.ObjectMapper;

import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.extras.plugins.kdb.exec.KdbSchema;
import com.dremio.extras.plugins.kdb.exec.KdbTable;
import com.dremio.extras.plugins.kdb.exec.c;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.dataset.proto.ScanStats;
import com.dremio.service.namespace.dataset.proto.ScanStatsType;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.protostuff.ByteString;

/**
 * kdb table
 */
public class KdbTableDefinition implements SourceTableDefinition {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final NamespaceKey key;
    private final String name;
    private final String tableName;
    private KdbSchema connection;
    private boolean built = false;
    private DatasetConfig dataset;
    private ArrayList<DatasetSplit> splits;

    public KdbTableDefinition(String name, String tableName, KdbSchema connection) {
        this.name = name;
        this.tableName = tableName;
        this.connection = connection;
        key = new NamespaceKey(ImmutableList.of(name, tableName));
    }

    @Override
    public NamespaceKey getName() {
        return key;
    }

    @Override
    public DatasetConfig getDataset() throws Exception {
        buildIfNecessary();
        return dataset;
    }

    @Override
    public boolean isSaveable() {
        return true;
    }

    @Override
    public DatasetType getType() {
        return DatasetType.PHYSICAL_DATASET;
    }

    private void buildIfNecessary() throws Exception {
        if (built) {
            return;
        }

        populate();
        built = true;
    }

    private Pair<BatchSchema, List<String>> getSchema() throws SQLException {
        SchemaBuilder builder = BatchSchema.newBuilder();
        KdbTable table = connection.getTable(tableName);
        List<String> symbolList = Lists.newArrayList();
        RelDataType rowType = table.getRowType(new JavaTypeFactoryImpl());
        for (String col : rowType.getFieldNames()) {
            RelDataTypeField field = rowType.getField(col, false, false);
            RelDataType value = field.getValue();
            Field arrowField;
            if (value.getSqlTypeName().equals(SqlTypeName.OTHER)) {
                Class clazz = ((RelDataTypeFactoryImpl.JavaType) (value)).getJavaClass();
                arrowField = KdbSchemaConverter.getArrowFieldFromJavaClass(col, clazz);
            } else {
                SqlTypeName typeName = field.getValue().getSqlTypeName();
                arrowField = KdbSchemaConverter.getArrowFieldFromJdbcType(col, typeName);
                if (SqlTypeName.SYMBOL.equals(typeName)) {
                    symbolList.add(col);
                }
            }

            builder.addField(arrowField);
        }
        return Pair.of(builder.build(), symbolList);
    }

    private void populate() throws Exception {


        dataset = new DatasetConfig().setPhysicalDataset(new PhysicalDataset())
                .setId(new EntityId().setId(UUID.randomUUID().toString()))
                .setFullPathList(key.getPathComponents())
                .setType(DatasetType.PHYSICAL_DATASET)
                .setName(name);
        dataset.setReadDefinition(new ReadDefinition());
        Pair<BatchSchema, List<String>> schemaList = getSchema();
        BatchSchema schema = schemaList.left;

        dataset.setRecordSchema(schema.toByteString());

        KdbXattr xattr = new KdbXattr();
        xattr.symbolList = schemaList.right;
        xattr.version = version();
        xattr.isPartitioned = isPartitioned();
        if (xattr.isPartitioned) {
            xattr.partitionColumn = "date"; //todo
        }

        List<String> partitions = partitionColumns();
        dataset.setReadDefinition(new ReadDefinition()
                .setPartitionColumnsList(partitions)
                .setSortColumnsList(sortedColumns())
                .setLastRefreshDate(System.currentTimeMillis())
                .setReadSignature(null)
                .setExtendedProperty(ByteString.copyFrom(MAPPER.writeValueAsString(xattr).getBytes()))
                .setScanStats(new ScanStats()
                        .setRecordCount(1000L)//todo
                        .setType(ScanStatsType.NO_EXACT_ROW_COUNT)
                        .setScanFactor(ScanCostFactor.OTHER.getFactor())

                )
        );
        populateSplits(partitions);
    }

    private List<String> sortedColumns() {
        Map<String, String> attrs = connection.getTable(tableName).getAttrs(new JavaTypeFactoryImpl());
        List<String> cols = Lists.newArrayList();
        for (Map.Entry<String, String> e : attrs.entrySet()) {
            if (e.getValue().contains("s")) {
                cols.add(e.getKey());
            }
        }
        return cols;
    }

    private double version() {
        try {
            double version = connection.getVersion();
            return version;
        } catch (IOException | c.KException e) {
            return -1;
        }
    }

    private boolean isPartitioned() {
        try {
            boolean version = connection.getPartitioned(tableName);
            return version;
        } catch (IOException | c.KException e) {
            return false;
        }
    }

    private List<String> partitionColumns() throws SQLException {
        Map<String, String> attrs = connection.getTable(tableName).getAttrs(new JavaTypeFactoryImpl());
        List<String> cols = Lists.newArrayList();
        if (attrs.keySet().contains("date")) {
            cols.add("date");//todo assuming date partition (safe for most cases) and it goes first!
        }
        for (Map.Entry<String, String> e : attrs.entrySet()) {
            if (e.getValue().contains("p")) {
                cols.add(e.getKey());
            }
        }
        return cols;
    }

    private void populateSplits(List<String> partitions) {
        splits = new ArrayList<>();
        if (partitions == null || partitions.isEmpty()) {
            splits.add(new DatasetSplit()
                    .setSize(11L)
                    .setSplitKey("1")
            );
        } else {
            //todo build metadata store to hold count per partition
            //split into datasets based on partitions and make sure Dremio allocates the partition ranges to executors sensibly
            splits.add(new DatasetSplit()
                    .setSize(11L)
                    .setSplitKey("1")
            );
        }

    }

    @Override
    public List<DatasetSplit> getSplits() throws Exception {
        buildIfNecessary();
        return splits;
    }

    /**
     * extra attributes for kdb
     */
    public static class KdbXattr {
        private double version;
        private boolean isPartitioned;
        private boolean isSplayed;
        private String partitionColumn;
        private List<String> symbolList = Lists.newArrayList();

        public KdbXattr() {

        }

        public double getVersion() {
            return version;
        }

        public void setVersion(double version) {
            this.version = version;
        }

        public boolean isPartitioned() {
            return isPartitioned;
        }

        public void setPartitioned(boolean partitioned) {
            isPartitioned = partitioned;
        }

        public boolean isSplayed() {
            return isSplayed;
        }

        public void setSplayed(boolean splayed) {
            isSplayed = splayed;
        }

        public String getPartitionColumn() {
            return partitionColumn;
        }

        public void setPartitionColumn(String partitionColumn) {
            this.partitionColumn = partitionColumn;
        }

        public List<String> getSymbolList() {
            return symbolList;
        }

        public void setSymbolList(List<String> symbolList) {
            this.symbolList = symbolList;
        }
    }

}
