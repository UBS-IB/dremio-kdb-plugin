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
package com.dremio.extras.plugins.kdb;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.ArrowType.Binary;
import org.apache.arrow.vector.types.pojo.ArrowType.Bool;
import org.apache.arrow.vector.types.pojo.ArrowType.Date;
import org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint;
import org.apache.arrow.vector.types.pojo.ArrowType.Int;
import org.apache.arrow.vector.types.pojo.ArrowType.Time;
import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp;
import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.ImmutableList;

/**
 * type converter for kdb->sql and back
 */
public final class KdbSchemaConverter {
    private KdbSchemaConverter() {
    }

    public static Field getArrowFieldFromJdbcType(String name, SqlTypeName typeInfo) {
        switch (typeInfo) {
            case BOOLEAN:
                return new Field(name, true, new Bool(), null);
            case TINYINT:
                return new Field(name, true, new Int(8, true), null);
            case SMALLINT:
                return new Field(name, true, new Int(16, true), null);

            case INTEGER:
                return new Field(name, true, new Int(32, true), null);

            case BIGINT:
                return new Field(name, true, new Int(64, true), null);

            case FLOAT:
                return new Field(name, true, new FloatingPoint(FloatingPointPrecision.SINGLE), null);

            case DOUBLE:
                return new Field(name, true, new FloatingPoint(FloatingPointPrecision.DOUBLE), null);

            case DATE:
                return new Field(name, true, new Date(DateUnit.MILLISECOND), null);
            case TIME:
                return new Field(name, true, new Time(TimeUnit.MILLISECOND, 32), null);
            case TIMESTAMP:
                return Field.nullable(name, new Timestamp(TimeUnit.MILLISECOND, null));

            case BINARY:
                return new Field(name, true, new Binary(), null);
//      case DECIMAL: {
//        typeInfo.
//        DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) pTypeInfo;
//        return new Field(name, true, new Decimal(decimalTypeInfo.getPrecision(), decimalTypeInfo.getScale()), null);
//      }
            case VARCHAR:
            case CHAR: {
                return new Field(name, true, new Utf8(), null);
            }
            case ARRAY:
            default:
                // fall through.
        }


        return null;
    }

    public static Field getArrowFieldFromJavaClass(String col, Class clazz) {
        if (clazz.equals(int[].class)) {
            Field child = getArrowFieldFromJdbcType("$data$", SqlTypeName.INTEGER);
            return new Field(col, true, new ArrowType.List(), ImmutableList.of(child));
        } else if (clazz.equals(double[].class)) {
            Field child = getArrowFieldFromJdbcType("$data$", SqlTypeName.DOUBLE);
            return new Field(col, true, new ArrowType.List(), ImmutableList.of(child));
        } else if (clazz.equals(short[].class)) {
            Field child = getArrowFieldFromJdbcType("$data$", SqlTypeName.SMALLINT);
            return new Field(col, true, new ArrowType.List(), ImmutableList.of(child));
        } else if (clazz.equals(java.sql.Timestamp[].class)) {
            Field child = getArrowFieldFromJdbcType("$data$", SqlTypeName.TIMESTAMP);
            return new Field(col, true, new ArrowType.List(), ImmutableList.of(child));
        }
        return null;
    }
}
