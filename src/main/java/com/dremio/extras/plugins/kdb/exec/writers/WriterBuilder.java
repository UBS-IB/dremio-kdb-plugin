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
package com.dremio.extras.plugins.kdb.exec.writers;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.extras.plugins.kdb.exec.KdbRecordReader;
import com.dremio.extras.plugins.kdb.exec.c;
import com.dremio.extras.plugins.kdb.exec.gwriters.BooleanArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.ByteArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.DateArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.DoubleArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.FloatArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.IntArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.LongArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.MinuteArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.MonthArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.SecondArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.ShortArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.TimeArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.TimespanArrowWriter;
import com.dremio.extras.plugins.kdb.exec.gwriters.TimestampArrowWriter;

/**
 * builder method for arrow writers
 */
public final class WriterBuilder {

    private WriterBuilder() {

    }

    public static ArrowWriter build(SchemaPath field, KdbRecordReader.VectorGetter vectorGetter, String name, Object val) {
        if (val instanceof boolean[]) {
            return new BooleanArrowWriter(field, vectorGetter, name, (boolean[]) val, new ArrowType.Bool());
        }
        if (val instanceof byte[]) {
            return new ByteArrowWriter(field, vectorGetter, name, (byte[]) val, new ArrowType.Int(8, true));
        }
        if (val instanceof short[]) {
            return new ShortArrowWriter(field, vectorGetter, name, (short[]) val, new ArrowType.Int(16, true));
        }
        if (val instanceof int[]) {
            return new IntArrowWriter(field, vectorGetter, name, (int[]) val, new ArrowType.Int(32, true));
        }
        if (val instanceof long[]) {
            return new LongArrowWriter(field, vectorGetter, name, (long[]) val, new ArrowType.Int(64, true));
        }
        if (val instanceof float[]) {
            return new FloatArrowWriter(field, vectorGetter, name, (float[]) val, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
        }
        if (val instanceof double[]) {
            return new DoubleArrowWriter(field, vectorGetter, name, (double[]) val, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
        }
        if (val instanceof Date[]) {
            return new DateArrowWriter(field, vectorGetter, name, (Date[]) val, new ArrowType.PrimitiveType.Date(DateUnit.MILLISECOND));
        }
        if (val instanceof Time[]) {
            return new TimeArrowWriter(field, vectorGetter, name, (Time[]) val, new ArrowType.PrimitiveType.Time(TimeUnit.MILLISECOND, 32));
        }
        if (val instanceof Timestamp[]) {
            return new TimestampArrowWriter(field, vectorGetter, name, (Timestamp[]) val, new ArrowType.PrimitiveType.Timestamp(TimeUnit.MILLISECOND, null));
        }
        if (val instanceof c.Timespan[]) {
            return new TimespanArrowWriter(field, vectorGetter, name, (c.Timespan[]) val, new ArrowType.PrimitiveType.Timestamp(TimeUnit.MILLISECOND, null));
        }
        if (val instanceof c.Minute[]) {
            return new MinuteArrowWriter(field, vectorGetter, name, (c.Minute[]) val, new ArrowType.PrimitiveType.Time(TimeUnit.MILLISECOND, 32));
        }
        if (val instanceof c.Month[]) {
            return new MonthArrowWriter(field, vectorGetter, name, (c.Month[]) val, new ArrowType.PrimitiveType.Date(DateUnit.MILLISECOND));
        }
        if (val instanceof c.Second[]) {
            return new SecondArrowWriter(field, vectorGetter, name, (c.Second[]) val, new ArrowType.PrimitiveType.Timestamp(TimeUnit.MILLISECOND, null));
        }
        if (val instanceof String[]) {
            return new StringArrowWriter(field, vectorGetter, name, (String[]) val, new ArrowType.PrimitiveType.Utf8());
        }
        if (val instanceof char[]) {
            return new StringArrowWriter(field, vectorGetter, name, (char[]) val, new ArrowType.PrimitiveType.Utf8());
        }
        if (val instanceof Object[]) {
            return arrayBuild(field, vectorGetter, name, (Object[]) val);
        }
        throw new UnsupportedOperationException();
    }

    private static ArrowWriter arrayBuild(SchemaPath field, KdbRecordReader.VectorGetter vectorGetter, String name, Object[] val) {
        if (val.length > 0) {
            ArrowWriter writer = build(field, vectorGetter, name, val[0]);
            if (writer instanceof StringArrowWriter && val[0] instanceof char[]) {
                return new StringArrowWriter(field, vectorGetter, name, val, new ArrowType.PrimitiveType.Utf8());
            }
            return new ArrowArrayWriter(field, vectorGetter, name, val, writer);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
