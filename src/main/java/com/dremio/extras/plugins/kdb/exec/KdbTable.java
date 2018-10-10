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

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Table based on a kdb table.
 */
public class KdbTable extends AbstractTable
        implements TranslatableTable {
    private static final Map<Character, SqlTypeName> ALL_TYPES = ImmutableMap.<Character, SqlTypeName>builder()
            .put('t', SqlTypeName.TIME)
            .put('s', SqlTypeName.SYMBOL)
            .put('i', SqlTypeName.INTEGER)
            .put('f', SqlTypeName.DOUBLE)
            .put('j', SqlTypeName.BIGINT)
            .put('C', SqlTypeName.VARCHAR)
//            .put('', SqlTypeName.CHAR)
            .put(' ', SqlTypeName.VARCHAR)
            .put('p', SqlTypeName.TIMESTAMP)
            .put('n', SqlTypeName.TIMESTAMP)
            .put('d', SqlTypeName.DATE)
            .put('z', SqlTypeName.TIMESTAMP)
            .put('h', /*actually a short but dremio doesn't support */ SqlTypeName.INTEGER)
            .put('x', /*actually a byte but dremio doesn't support */ SqlTypeName.INTEGER)
            .put('b', SqlTypeName.BOOLEAN)
            .put('e', SqlTypeName.FLOAT)
            .put('c', SqlTypeName.VARCHAR)
//    .put('I', SqlTypeName.ARRAY)
//    .put('F', SqlTypeName.ARRAY)
//    .put('H', SqlTypeName.ARRAY)
//    .put('P', SqlTypeName.ARRAY)
            .build();
    private static final Map<Character, Class> ALL_VECTOR_TYPES = ImmutableMap.<Character, Class>builder()
            .put('I', int[].class)
            .put('F', double[].class)
            .put('E', float[].class)
            .put('H', /*short[] dremio doesn't support short*/int[].class)
            .put('J', long[].class)
            .put('X', /*byte[] dremio doesn't support short*/byte[][].class)
            .put('T', Time[].class)
            .put('D', Date[].class)
            .put('N', Timestamp[].class)
            .put('Z', Timestamp[].class)
            .put('P', Timestamp[].class)
            .put('S', String[].class)
            .put('B', boolean[].class)
            .build();
    private final String collectionName;
    private final KdbConnection conn;
    private final RelProtoDataType protoRowType;
    private RelDataType rowType = null;
    private Map<String, String> attrs;

    /**
     * Creates a KdbTable.
     */
    KdbTable(String collectionName, KdbConnection conn, RelProtoDataType protoRowType) {
        super();
        this.collectionName = collectionName;
        this.conn = conn;
        this.protoRowType = protoRowType;
    }

    public String toString() {
        return "KdbTable {" + collectionName + "}";
    }

    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        fillMembers(typeFactory);
        return rowType;
    }

    public Map<String, String> getAttrs(RelDataTypeFactory typeFactory) {
        fillMembers(typeFactory);
        return attrs;
    }

    private void fillMembers(RelDataTypeFactory typeFactory) {
        if (rowType == null) {
            Pair<RelDataType, Map<String, String>> pair = getRowTypeImpl(typeFactory);
            rowType = pair.left;
            attrs = pair.right;
        }
    }

    private Pair<RelDataType, Map<String, String>> getRowTypeImpl(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return Pair.of(protoRowType.apply(typeFactory), null);
        }
        final List<RelDataType> types = new ArrayList<>();
        final Map<String, String> myAttrs = Maps.newHashMap();
        final List<String> names = new ArrayList<>();
        Triple<String[], char[], String[]> o = conn.getSchema(collectionName);
        for (char c : o.getMiddle()) {
            RelDataType type;
            if (ALL_TYPES.containsKey(c)) {
                type = typeFactory.createSqlType(ALL_TYPES.get(c));
            } else if (ALL_VECTOR_TYPES.containsKey(c)) {
                type = typeFactory.createJavaType(ALL_VECTOR_TYPES.get(c));
            } else {
                throw new UnsupportedOperationException("Have never seen this data type before " + c);

            }

            types.add(type);
        }
        names.addAll(Arrays.asList(o.getLeft()));
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            String attr = o.getRight()[i];
            myAttrs.put(name, attr);
        }

        return Pair.of(typeFactory.createStructType(Pair.zip(names, types)), myAttrs);
    }

    public RelNode toRel(
            RelOptTable.ToRelContext context,
            RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new KdbTableScan(cluster, cluster.traitSetOf(),
                relOptTable, this, null);
    }

}

// End KdbTable.java
