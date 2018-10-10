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
package com.dremio.extras.plugins.kdb.exec.nullCheck;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.extras.plugins.kdb.exec.c;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.ByteNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.DateNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.DoubleNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.FloatNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.IntegerNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.LongNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.MinuteNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.MonthNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.SecondNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.ShortNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.TimeNullCheck;
import com.dremio.extras.plugins.kdb.exec.gnullCheck.TimestampNullCheck;

/**
 * scan kdb buffer for nulls
 */
public final class NullCheckBuilder {
    private NullCheckBuilder() {
    }

    public static NullCheck build(String name, Object val, UserBitShared.SerializedField serializedField) {
        if (val instanceof double[]) {
            return new DoubleNullCheck();
        } else if (val instanceof float[]) {
            return new FloatNullCheck();
        } else if (val instanceof int[]) {
            return new IntegerNullCheck();
        } else if (val instanceof short[]) {
            return new ShortNullCheck();
        } else if (val instanceof char[]) {
            return new StringNullCheck(serializedField);
        } else if (val instanceof byte[]) {
            return new ByteNullCheck();
        } else if (val instanceof boolean[]) {
            return new BooleanNullCheck();
        } else if (val instanceof long[]) {
            return new LongNullCheck();
        } else if (val instanceof Timestamp[]) {
            return new TimestampNullCheck();
        } else if (val instanceof c.Timespan[]) {
            return new TimestampNullCheck();
        } else if (val instanceof c.Minute[]) {
            return new MinuteNullCheck();
        } else if (val instanceof c.Month[]) {
            return new MonthNullCheck();
        } else if (val instanceof c.Second[]) {
            return new SecondNullCheck();
        } else if (val instanceof String[]) {
            return new StringNullCheck(serializedField);
        } else if (val instanceof Time[]) {
            return new TimeNullCheck();
        } else if (val instanceof Date[]) {
            return new DateNullCheck();
        } else if (val instanceof Object[]) {
            if (((Object[]) val).length == 0) {
                return new StringNullCheck(serializedField);
            } else if (((Object[]) val)[0] instanceof char[]) {
                return new StringNullCheck(serializedField);
            } else {
                NullCheck inner = null;
                try {
                    inner = build(name, ((Object[]) val)[0], serializedField);
                } catch (IndexOutOfBoundsException e) {
                    inner = new NullNullCheck();
                }
                return new ArrayNullCheck(serializedField, inner);
            }
        } else {
            throw new UnsupportedOperationException(name);
        }
    }
}
