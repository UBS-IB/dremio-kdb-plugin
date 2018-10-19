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
<@pp.dropOutputFile />



<#list writer.types as type>
<#if type.major == "Date">

<#if type.rename == true>
<@pp.changeOutputFile name="/com/dremio/extras/plugins/kdb/exec/gwriters/${type.renameVal}ArrowWriter.java" />
<#else>
<@pp.changeOutputFile name="/com/dremio/extras/plugins/kdb/exec/gwriters/${type.from}ArrowWriter.java" />
</#if>


<#include "/@includes/license.ftl" />


package com.dremio.extras.plugins.kdb.exec.gwriters;

import java.io.IOException;

import ${type.import};

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.extras.plugins.kdb.exec.KdbRecordReader;
import com.dremio.extras.plugins.kdb.exec.allocators.Allocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.<#if type.rename == true>${type.renameVal}<#else>${type.from}</#if>Allocator;
import com.dremio.extras.plugins.kdb.exec.writers.ArrowWriter;
import com.dremio.extras.plugins.kdb.exec.writers.ArrowWriterHelper;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheck;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheckBuilder;

import io.netty.buffer.ArrowBuf;

public class <#if type.rename == true>${type.renameVal}<#else>${type.from}</#if>ArrowWriter implements ArrowWriter {
    private final KdbRecordReader.VectorGetter vector;
    private final SchemaPath field;
    private final String name;
    private final ${type.from}[] val;
    private final Allocator allocator;
    private final int size;
    private final int count;
    private final ArrowType.PrimitiveType type;

    public <#if type.rename == true>${type.renameVal}<#else>${type.from}</#if>ArrowWriter(SchemaPath field, KdbRecordReader.VectorGetter vector, String name, ${type.from}[] val, ArrowType.PrimitiveType type) {
        this.field = field;
        this.vector = vector;
        this.name = name;
        this.val = val;
        this.type = type;
        count = val.length;
        allocator = new <#if type.rename == true>${type.renameVal}<#else>${type.from}</#if>Allocator();
        size = count * allocator.offset();
    }

    @Override
    public int write(BufferAllocator bufferAllocator) throws IOException {
        int dataSize;
        int count = val.length;
        Allocator allocator = new <#if type.rename == true>${type.renameVal}<#else>${type.from}</#if>Allocator();
        int size = count * allocator.offset();


        UserBitShared.SerializedField serializedField = ArrowWriterHelper.getSerializedField(size, count, field);

        int offsetLength = 0;
        int dataLength = serializedField.getChild(1).getBufferLength();
        int bitsLength = serializedField.getChild(0).getBufferLength();

        dataSize = serializedField.getValueCount();
        NullCheck check = NullCheckBuilder.build(name, val, serializedField);

        try (ArrowBuf buf = bufferAllocator.buffer(dataLength + bitsLength + offsetLength)) {
            ArrowBuf outBuf = buf.slice(bitsLength, dataLength + offsetLength).writerIndex(0);
            allocator.get(val, dataLength + offsetLength, dataSize, outBuf, null);
            ArrowWriterHelper.getNulls(buf, check, dataLength, bitsLength, offsetLength);
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
    public ArrowType getType() {
        return type;
    }

}
</#if> <#-- type.major -->
</#list>