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



package com.dremio.extras.plugins.kdb.exec.writers;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.vector.complex.fn.ArrowBufInputStream;
import com.dremio.extras.plugins.kdb.exec.KdbRecordReader;
import com.dremio.extras.plugins.kdb.exec.allocators.Allocator;
import com.dremio.extras.plugins.kdb.exec.allocators.CharAllocator;
import com.dremio.extras.plugins.kdb.exec.allocators.StringAllocator;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheck;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheckBuilder;

import io.netty.buffer.ArrowBuf;

/**
 * Arrow writer for strings
 */
public class StringArrowWriter implements ArrowWriter {
    private final KdbRecordReader.VectorGetter vector;
    private final SchemaPath field;
    private final String name;
    private final Object val;
    private final Allocator allocator;
    private ArrowType.PrimitiveType type;
    private final int size;
    private final int count;

    public StringArrowWriter(SchemaPath field, KdbRecordReader.VectorGetter vector, String name, Object val, ArrowType.PrimitiveType type) {
        this.field = field;
        this.vector = vector;
        this.name = name;
        this.val = val;
        if (val instanceof Object[]) {
            count = ((Object[]) val).length;
        } else {
            count = Math.max(1, (val instanceof char[]) ? ((char[]) val).length : ((String[]) val).length);
        }
        allocator = (val instanceof char[]) ? new CharAllocator() : new StringAllocator();
        this.type = type;
        size = count * allocator.offset();
    }

    @Override
    public int write(BufferAllocator bufferAllocator) throws IOException {
        int dataSize;

        UserBitShared.SerializedField serializedField = ArrowWriterHelper.getSerializedField(size, count, field, val, null);

        int offsetLength = serializedField.getChild(1).getChild(0).getBufferLength();
        int dataLength = serializedField.getChild(1).getBufferLength() - offsetLength;
        int bitsLength = serializedField.getChild(0).getBufferLength();

        dataSize = serializedField.getValueCount();
        NullCheck check = NullCheckBuilder.build(name, val, serializedField);

        try (ArrowBuf buf = bufferAllocator.buffer(dataLength + bitsLength + offsetLength)) {
            ArrowBuf outBuf = buf.slice(bitsLength, dataLength + offsetLength).writerIndex(0);
            allocator.get(val, dataLength + offsetLength, dataSize, outBuf, null);
            ArrowBufInputStream ab = ArrowBufInputStream.getStream(bitsLength, dataLength + bitsLength + offsetLength, buf);
            ArrowBuf bits = buf.slice(0, bitsLength).writerIndex(0);
            ArrowWriterHelper.getNulls(ab, bits, check);
            TypeHelper.load(vector.get(), serializedField, buf);
        }
        return dataSize;
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public int getCount() {
        return count;
    }

    @Override
    public Allocator getAllocator() {
        return allocator;
    }

    @Override
    public ArrowType.PrimitiveType getType() {
        return type;
    }
}
