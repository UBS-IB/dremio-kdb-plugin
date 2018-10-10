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

import java.io.IOException;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.util.Pair;
import org.apache.http.util.Asserts;
import org.junit.After;
import org.junit.Test;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.exec.util.VectorUtil;
import com.google.common.collect.Maps;

/**
 * test all arrow types
 */
public class KdbReaderTest extends QControllerSimple {

    BufferAllocator allocator;
    Map<String, KdbRecordReader.VectorGetter> vectors = Maps.newHashMap();
    Map<String, SchemaPath> fields = Maps.newHashMap();

    public void build(Map<String, Field> columns) {
        allocator = new RootAllocator(100000000);
        TestOutputMutator output = new TestOutputMutator(allocator);
        for (String name : columns.keySet()) {
            Field f = columns.get(name);
            vectors.put(name, new KdbRecordReader.VectorGetter(output, f));
            fields.put(name, SchemaPath.getSimplePath(name));

        }
    }

    @After
    public void destroy() {
        vectors.values().stream().forEach(vectorGetter -> vectorGetter.get().close());
        allocator.close();
        allocator = null;
        vectors.clear();
        fields.clear();
    }

    @Test
    public void buildByte() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("byte");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildShort() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("short");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildInt() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("int");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildLong() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("long");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildFloat() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("float");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildDate() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("date");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }


    @Test
    public void buildTime() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("time");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildTimestamp() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("timestamp");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildMinute() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("minute");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildTimespan() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("timespan");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildSecond() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("second");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildDouble() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("double");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildMonth() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("month");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildBoolean() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("boolean");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildString() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("string");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    @Test
    public void buildChar() throws IOException {
        Pair<Map<String, Field>, c.Flip> pair = FlipMaker.primitiveMaker("char");
        build(pair.left);
        int resultCount = KdbReader.getFlip(pair.right, allocator, fields, vectors);
        printResult(resultCount);
        Asserts.check(1000 == resultCount, "wrong number of messages processed");
    }

    private int[] columnWidths = new int[]{8};

    protected int printResult(int resultCount) throws SchemaChangeException {
        VectorContainer vc = new VectorContainer();
        vc.add(vectors.values().iterator().next().get());
        vc.buildSchema();
        vc.setRecordCount(10);
        VectorUtil.showVectorAccessibleContent(vc, columnWidths);
        System.out.println("Total record count: " + resultCount);
        return resultCount;
    }
}
