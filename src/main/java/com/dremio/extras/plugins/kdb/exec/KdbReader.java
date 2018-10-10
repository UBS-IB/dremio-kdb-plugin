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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.SchemaPath;
import com.dremio.extras.plugins.kdb.exec.writers.ArrowWriter;
import com.dremio.extras.plugins.kdb.exec.writers.WriterBuilder;
import com.google.common.annotations.VisibleForTesting;

/**
 * turn kdb query into arrow buffer
 */
public class KdbReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbReader.class);
    private final String query;
    private final KdbConnection connection;
    private final String uuid;
    private final Map<String, KdbRecordReader.VectorGetter> vectors;
    private final byte[] copyBuffer = new byte[64 * 1024];
    private int searchSize = 0;
    private int currentLocation = 0;
    private boolean first = true;
    private Object resultSet;
    private Object error;
    private int index = 0;
    private Map<String, SchemaPath> fields;
    private long totalSize;
    private Throwable t;

    public KdbReader(
            String query, KdbConnection connection, Map<String, KdbRecordReader.VectorGetter> vectors, Map<String, SchemaPath> fields) {
        this.query = query;
        this.connection = connection;
        uuid = new UUID(DateTime.now().getMillis(), query.hashCode()).toString().replace("-", "_");
        this.vectors = vectors;
        this.fields = fields;
    }

    public void setBatchSize(int searchSize) {
        this.searchSize = searchSize;
    }


    private String getQuery() throws IOException, c.KException {
        if (searchSize == currentLocation) {
            if (first) {
                first = false;
                return query;
            } else {
                return "";
            }
        }
        if (first) {
            connection.select(".temp._" + uuid + ":(" + query + ")");
            totalSize = getTotalSizeImpl();
            first = false;
        }
        final String search = "select[" + currentLocation + "," + (currentLocation + searchSize) + "] from .temp._" + uuid;
        currentLocation += searchSize;
        return search;
    }

    public void nextQuery() throws IOException, c.KException {
        resultSet = connection.select(getQuery());
        index = 0;
    }

    public void delete() throws IOException, c.KException {
        try {
            connection.select("@[`.temp;`$\"_" + uuid + "\";0#]");
        } catch (Throwable t) {
            connection.select(".temp._" + uuid + ":()");
        }
    }


    public Pair<ReadState, Integer> write(BufferAllocator bufferAllocator) {
        int read = 0;
        if (index >= totalSize) {
            return new ImmutablePair<>(ReadState.END_OF_STREAM, read);
        }

        try {
            read = getCurrent(bufferAllocator);
            index += read;
            //} catch (IOException e) {
//      return new ImmutablePair<>(ReadState.END_OF_STREAM, read);
        } catch (Throwable t) {
            error = resultSet;
            this.t = t;
            return new ImmutablePair<>(ReadState.ERROR, read);
        }
        return new ImmutablePair<>(ReadState.WRITE_SUCCEED, index);
    }

    public String getUuid() {
        return uuid;
    }

    public String getError() {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        this.t.printStackTrace(printWriter);
        return writer.toString();
    }

    public Throwable getThrowable() {
        return t;
    }

    @VisibleForTesting
    public static int getFlip(c.Flip resultSet, BufferAllocator allocator, Map<String, SchemaPath> fields, Map<String, KdbRecordReader.VectorGetter> vectors) throws IOException {
        int dataSize = 0;
        for (int i = 0; i < resultSet.y.length; i++) {
            String name = resultSet.x[i].replace("xx_xx", "$");
            Object val = resultSet.y[i];
            ArrowWriter writer = WriterBuilder.build(fields.get(name), vectors.get(name), name, val);
            dataSize = writer.write(allocator);
        }
        return dataSize;
    }

    private int getCurrent(BufferAllocator bufferAllocator) throws ArrayIndexOutOfBoundsException, IOException {
        if (resultSet instanceof c.Flip) {
            int i = getFlip((c.Flip) resultSet, bufferAllocator, fields, vectors);
            return i;
        }
        if (resultSet instanceof c.Dict) {
            int i = getDict((c.Dict) resultSet, bufferAllocator, fields, vectors);
            return i;
        }
        throw new IOException("cant deal with " + resultSet.getClass().toString());
    }

    private int getDict(c.Dict resultSet, BufferAllocator bufferAllocator, Map<String, SchemaPath> fields, Map<String, KdbRecordReader.VectorGetter> vectors) throws IOException {
        c.Flip keys = (c.Flip) resultSet.x;
        c.Flip values = (c.Flip) resultSet.y;
        int i = getFlip(keys, bufferAllocator, fields, vectors);
        int ii = getFlip(values, bufferAllocator, fields, vectors);
        assert i == ii;
        return i;
    }

    public long getTotalSize() {
        return totalSize;
    }

    public long getTotalSizeImpl() {
        try {
            Object count = connection.select("count .temp._" + uuid);
            if (count instanceof Long) {
                return (long) count;
            }
            if (count instanceof Integer) {
                return (int) count;
            }
        } catch (IOException e) {
            LOGGER.error("Unable to capture count of table .temp._" + uuid, e);
            return 0;
        }
        return 0;
    }

    enum ReadState {
        END_OF_STREAM, WRITE_SUCCEED, ERROR
    }


}
