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

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.types.TypeProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.vector.complex.fn.ArrowBufInputStream;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheck;
import com.google.common.base.Throwables;

import io.netty.buffer.ArrowBuf;

/**
 * static helper class
 */
public final class ArrowWriterHelper {

    private ArrowWriterHelper() {

    }

    private static UserBitShared.SerializedField getArraySerializedOffsets(int count) {
        UserBitShared.SerializedField.Builder offsetsBuilder = UserBitShared.SerializedField.newBuilder();
        offsetsBuilder.setBufferLength(count * 4 + 4);
        offsetsBuilder.setValueCount(count + 1);
        UserBitShared.SerializedField offset = offsetsBuilder.build();
        return offset;
    }

    public static UserBitShared.SerializedField getSerializedArray(int count, SchemaPath field, Object val, TypeProtos.MajorType name) {
        int innerCount = 0;
        int innerSize = 0;
        UserBitShared.SerializedField bits = getSerializedNulls(count);
        UserBitShared.SerializedField offsets = getArraySerializedOffsets(count);
        for (Object o : (Object[]) val) {
            ArrowWriter builder = WriterBuilder.build(field, null, null, o);
            innerCount += builder.getCount();
            innerSize += builder.getSize();
        }
        UserBitShared.SerializedField vals = getSerializedField(innerSize, innerCount, field, (innerSize < 0) ? val : null, name);

        return UserBitShared.SerializedField.newBuilder()
                .addChild(offsets)
                .addChild(bits)
                .addChild(vals)
                .setValueCount(count)
                .setMajorType(name)
                .build();
    }

    public static UserBitShared.SerializedField getSerializedField(int size, int count, SchemaPath field) {
        return getSerializedField(size, count, field, null, null);
    }

    public static UserBitShared.SerializedField getSerializedField(int size, int count, SchemaPath field, Object val, TypeProtos.MajorType name) {
        UserBitShared.SerializedField bits = getSerializedNulls(count);
        UserBitShared.SerializedField vals = (val != null) ? getStringSerializedValue(count, val, field) : getSerializedValue(count, size, field);
        UserBitShared.SerializedField.Builder builder = UserBitShared.SerializedField.newBuilder()
                .addChild(bits)
                .addChild(vals)
                .setValueCount(vals.getValueCount())
                .setBufferLength(vals.getBufferLength() + bits.getBufferLength());
        if (name != null) {
            builder.setMajorType(name);
        }
        return builder.build();
    }

    static UserBitShared.SerializedField getSerializedNulls(int count) {
        UserBitShared.SerializedField.Builder builder = UserBitShared.SerializedField.newBuilder();
        builder.setValueCount(count);
        double bytes = count / 8.;
        count = (int) Math.ceil(bytes);
        builder.setBufferLength(count);
        return builder.build();
    }

    static UserBitShared.SerializedField getSerializedValue(int count, int size, SchemaPath field) {
        UserBitShared.SerializedField.Builder builder = UserBitShared.SerializedField.newBuilder();
        builder.setNamePart(field.getAsNamePart());
        builder.setBufferLength(size);
        builder.setValueCount(count);
        return builder.build();
    }

    public static void getNulls(ArrowBuf in, NullCheck check, int dataLength, int dataSize, int offsetLength) throws IOException {
        ArrowBufInputStream ab = ArrowBufInputStream.getStream(dataSize, dataLength + dataSize + offsetLength, in);
        ArrowBuf buf = in.slice(0, dataSize).writerIndex(0);
        getNulls(ab, buf, check);
    }

    public static void getNulls(ArrowBufInputStream dataBuf, ArrowBuf bitsBuffer, NullCheck check) throws IOException {
        byte value = 0;
        int i = 0;
        while (dataBuf.available() > 0) {
            boolean isNull = check.check(dataBuf);
            value += isNull ? (1 << i) : 0;
            i++;
            if (i == 8) {
                bitsBuffer.writeByte(value);
                value = 0;
                i = 0;
            }
        }
        if (i > 0) {
            bitsBuffer.writeByte(value);
        }
    }

    public static ArrowBuf makeNull(ArrowBuf bitsBuffer, int stop) {
        return (ArrowBuf) bitsBuffer.setZero(0, stop);
    }

    public static UserBitShared.SerializedField getStringSerializedValue(int count, Object val, SchemaPath field) {
        int size;
        UserBitShared.SerializedField.Builder offsetsBuilder = UserBitShared.SerializedField.newBuilder();
        offsetsBuilder.setBufferLength((count == 0) ? 0 : (count * 4 + 4));
        offsetsBuilder.setValueCount((count == 0) ? 0 : (count + 1));
        UserBitShared.SerializedField offset = offsetsBuilder.build();
        try {
            size = getStringSize(val);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
        UserBitShared.SerializedField.Builder builder = UserBitShared.SerializedField.newBuilder();
        builder.setNamePart(field.getAsNamePart());
        builder.setBufferLength(size + ((count == 0) ? 0 : (count + 1)) * 4);
        builder.setValueCount(count);
        builder.addChild(offset);
        return builder.build();
    }

    private static int getStringSize(Object val) throws UnsupportedEncodingException {
        int size = 0;
        if (val instanceof String[]) {
            for (String v : ((String[]) val)) {
                size += v.getBytes("UTF-8").length;
            }
            return size;
        } else if (val instanceof char[]) {
            for (char v : ((char[]) val)) {
                size += Character.toString(v).getBytes("UTF-8").length;
            }
            return size;
        } else if (val instanceof Object[]) {
            for (Object o : ((Object[]) val)) {
                size += getStringSize(o);
            }
            return size;
        }
        throw new UnsupportedEncodingException();
    }
}
