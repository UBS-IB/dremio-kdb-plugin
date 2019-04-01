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
import java.util.Map;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableMap;

/**
 * Helper class for generating test data
 */
public final class FlipMaker {

    private FlipMaker() {

    }

    public static Object[] getArrayFromType(ArrowType type, String typeName) {
        Object[] retVal = new Object[1];
        Object[] arrays = new Object[1000];
        int i = 0;
        while (i < 1000) {
            Object[] row = getRowsFromType(type, typeName);
            arrays[i] = row[0];
            i++;
        }
        retVal[0] = arrays;
        return retVal;
    }

    public static Object[] getRowsFromType(ArrowType type, String typeName) {
        Object[] retVal = new Object[1];

        switch (type.getTypeID()) {
            case Int:
                if (((ArrowType.Int) type).getBitWidth() == 32) {
                    retVal[0] = new int[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((int[]) retVal[0])[i] = i;
                        i++;
                    }
                    return retVal;
                } else if (((ArrowType.Int) type).getBitWidth() == 16) {
                    retVal[0] = new short[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((short[]) retVal[0])[i] = (short) i;
                        i++;
                    }
                    return retVal;
                } else if (((ArrowType.Int) type).getBitWidth() == 8) {
                    retVal[0] = new byte[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((byte[]) retVal[0])[i] = (byte) i;
                        i++;
                    }
                    return retVal;
                } else {
                    retVal[0] = new long[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((long[]) retVal[0])[i] = (long) i;
                        i++;
                    }
                    return retVal;
                }
            case FloatingPoint:
                if (((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE) {
                    retVal[0] = new double[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((double[]) retVal[0])[i] = i;
                        i++;
                    }
                    return retVal;
                } else {
                    retVal[0] = new float[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((float[]) retVal[0])[i] = i;
                        i++;
                    }
                    return retVal;
                }
            case Date: {
                if (typeName.equals("date")) {
                    retVal[0] = new Date[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((Date[]) retVal[0])[i] = new Date(new Date(2019 - 1900, 0, 1).getTime() + i * 10000);
                        i++;
                    }
                    return retVal;
                } else if (typeName.equals("month")) {
                    retVal[0] = new c.Month[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((c.Month[]) retVal[0])[i] = new c.Month(i);
                        i++;
                    }
                    return retVal;
                }
            }
            case Time: {
                if (typeName.equals("minute")) {
                    retVal[0] = new c.Minute[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((c.Minute[]) retVal[0])[i] = new c.Minute(i * 60);
                        i++;
                    }
                    return retVal;
                } else if (typeName.equals("time")) {
                    retVal[0] = new Time[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((Time[]) retVal[0])[i] = new Time(new Date(2019 - 1900, 0, 1).getTime() + i * 10000);
                        i++;
                    }
                    return retVal;
                }
            }
            case Timestamp: {
                if (typeName.equals("second")) {
                    retVal[0] = new c.Second[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((c.Second[]) retVal[0])[i] = new c.Second(i);
                        i++;
                    }
                    return retVal;
                } else if (typeName.equals("timestamp")) {
                    retVal[0] = new Timestamp[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((Timestamp[]) retVal[0])[i] = new Timestamp(new Timestamp(2019 - 1900, 0, 1, 0, 0, 0, 0).getTime() + i * 10000);
                        i++;
                    }
                    return retVal;
                } else if (typeName.equals("timespan")) {
                    retVal[0] = new c.Timespan[1000];
                    int i = 0;
                    while (i < 1000) {
                        ((c.Timespan[]) retVal[0])[i] = new c.Timespan(i);
                        i++;
                    }
                    return retVal;
                }
            }
            case Bool: {
                retVal[0] = new boolean[1000];
                int i = 0;
                while (i < 1000) {
                    ((boolean[]) retVal[0])[i] = i % 2 == 0;
                    i++;
                }
                return retVal;
            }
            case Utf8: {
                retVal[0] = (typeName.equals("char")) ? new char[1000] : new String[1000];
                int i = 0;
                while (i < 1000) {
                    if (typeName.equals("char")) {
                        ((char[]) retVal[0])[i] = 'a';
                    } else {
                        ((String[]) retVal[0])[i] = Integer.toString(i);
                    }
                    i++;
                }
                return retVal;
            }
        }
        return null;
    }

    public static Pair<Map<String, Field>, c.Flip> arrayMaker(String typeName) {
        ArrowType.PrimitiveType type = getType(typeName);
        Object[] rows = getArrayFromType(type, typeName);
        c.Flip flip = new c.Flip(new c.Dict(new String[]{"fieldname"}, rows));
        return Pair.of(ImmutableMap.of("fieldname", Field.nullablePrimitive("fieldname", type)),
                flip);
    }

    public static Pair<Map<String, Field>, c.Flip> primitiveMaker(String typeName) {
        ArrowType.PrimitiveType type = getType(typeName);
        Object[] rows = getRowsFromType(type, typeName);
        c.Flip flip = new c.Flip(new c.Dict(new String[]{"fieldname"}, rows));
        return Pair.of(ImmutableMap.of("fieldname", Field.nullablePrimitive("fieldname", type)),
                flip);
    }

    public static ArrowType.PrimitiveType getType(String type) {
        switch (type) {
            case "int":
                return new ArrowType.Int(32, true);
            case "long":
                return new ArrowType.Int(64, true);
            case "short":
                return new ArrowType.Int(16, true);
            case "byte":
                return new ArrowType.Int(8, true);
            case "double":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "float":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case "date":
            case "month":
                return new ArrowType.Date(DateUnit.MILLISECOND);
            case "minute":
            case "time":
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case "hour":
            case "second":
            case "timestamp":
            case "timespan":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "boolean":
                return new ArrowType.Bool();
            case "string":
            case "char":
                return new ArrowType.Utf8();
        }
        return null;
    }
}
