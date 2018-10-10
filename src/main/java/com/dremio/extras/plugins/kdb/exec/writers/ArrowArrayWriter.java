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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.util.MajorTypeHelper;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.vector.complex.fn.ArrowBufInputStream;
import com.dremio.extras.plugins.kdb.exec.KdbRecordReader;
import com.dremio.extras.plugins.kdb.exec.allocators.Allocator;
import com.dremio.extras.plugins.kdb.exec.nullCheck.ArrayNullCheck;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheck;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheckBuilder;

import io.netty.buffer.ArrowBuf;

/**
 * Arrow writer for ararys
 */
public class ArrowArrayWriter implements ArrowWriter {
    private final KdbRecordReader.VectorGetter vector;
    private final SchemaPath field;
    private final String name;
    private final Object[] val;
    private final UserBitShared.SerializedField serializedField;
    private ArrowWriter builder;

    public ArrowArrayWriter(SchemaPath field, KdbRecordReader.VectorGetter vector, String name, Object[] val, ArrowWriter builder) {
        this.field = field;
        this.vector = vector;
        this.name = name;
        this.val = val;
        this.builder = builder;
        int count = val.length;

        serializedField = ArrowWriterHelper.getSerializedArray(count, field, val, MajorTypeHelper.getMajorTypeForField(Field.nullable("name", builder.getType())));
    }

    @Override
    public int write(BufferAllocator bufferAllocator) throws IOException {
        int dataSize = 0;


        int offsetLength = serializedField.getChild(0).getBufferLength();
        int bitsLength = serializedField.getChild(1).getBufferLength();
        UserBitShared.SerializedField innerSerializedField = serializedField.getChild(2);
        int innerOffestLength = 0;
        int dataLength;
        int innerBitsLength;
        int innerDataLength;
        if (innerSerializedField.getChild(1).getChildCount() > 0) {
            innerOffestLength = innerSerializedField.getChild(1).getChild(0).getBufferLength();
            innerDataLength = innerSerializedField.getChild(1).getBufferLength() - innerOffestLength;
            innerBitsLength = innerSerializedField.getChild(0).getBufferLength();
        } else {
            innerDataLength = innerSerializedField.getChild(1).getBufferLength();
            innerBitsLength = innerSerializedField.getChild(0).getBufferLength();
        }

        dataLength = innerOffestLength + innerBitsLength + innerDataLength;
        dataSize = serializedField.getValueCount();
        NullCheck check = NullCheckBuilder.build(name, val, serializedField);
        NullCheck checkInner = ((ArrayNullCheck) check).getInner();

        try (ArrowBuf buf = bufferAllocator.buffer(dataLength + bitsLength + offsetLength)) {
            ArrowBuf outBuf = buf.slice(bitsLength + offsetLength + innerBitsLength, dataLength).writerIndex(0);
            ArrowBuf offsets = buf.slice(0, offsetLength).writerIndex(0);
            builder.getAllocator().get(val, innerDataLength + innerOffestLength, dataSize, outBuf, offsets);
            ArrayNullCheck.OffsetBuff offsetBuff = new ArrayNullCheck.OffsetBuff(buf.slice(0, offsetLength).writerIndex(offsetLength).readerIndex(0), builder.getAllocator().offset());
            ((ArrayNullCheck) check).setOffset(offsetBuff);


            ArrowWriterHelper.getNulls(
                    ArrowBufInputStream.getStream(bitsLength + offsetLength + innerBitsLength + innerOffestLength, bitsLength + offsetLength + dataLength, buf),
                    ArrowWriterHelper.makeNull(buf.slice(offsetLength, offsetLength + bitsLength).writerIndex(0), bitsLength),
                    check);
            ArrowWriterHelper.getNulls(
                    ArrowBufInputStream.getStream(bitsLength + offsetLength + innerBitsLength /*+ innerOffestLength*/, bitsLength + offsetLength + innerBitsLength + innerOffestLength + innerDataLength, buf),
                    ArrowWriterHelper.makeNull(buf.slice(bitsLength + offsetLength, innerBitsLength).writerIndex(0), innerBitsLength),
                    checkInner);
            TypeHelper.load(vector.get(), serializedField, buf);
        }
        return dataSize;
    }

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public int getCount() {
        return 0;
    }

    @Override
    public Allocator getAllocator() {
        return builder.getAllocator();
    }

    @Override
    public ArrowType.PrimitiveType getType() {
        return null;
    }
}
