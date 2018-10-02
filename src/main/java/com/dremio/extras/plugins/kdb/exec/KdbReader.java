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
package com.dremio.extras.plugins.kdb.exec;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.vector.complex.fn.ArrowBufInputStream;
import com.dremio.extras.plugins.kdb.exec.allocators.Allocator;
import com.dremio.extras.plugins.kdb.exec.allocators.BooleanAllocator;
import com.dremio.extras.plugins.kdb.exec.allocators.StringAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.ByteAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.DateAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.DoubleAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.IntAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.LongAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.MinuteAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.ShortAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.TimeAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.TimespanAllocator;
import com.dremio.extras.plugins.kdb.exec.gallocators.TimestampAllocator;
import com.dremio.extras.plugins.kdb.exec.nullCheck.ArrayNullCheck;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheck;
import com.dremio.extras.plugins.kdb.exec.nullCheck.NullCheckBuilder;
import com.google.common.base.Throwables;

import io.netty.buffer.ArrowBuf;

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
        PrintWriter printWriter= new PrintWriter(writer);
        this.t.printStackTrace(printWriter);
        return printWriter.toString();
    }

    private CountContainer getCountLength(String name, Object val) {
        int count = 0;
        int size = 0;
        Allocator a = null;
        DataType d = DataType.PRIMITIVE;

        if (val instanceof double[]) {
            count = ((double[]) val).length;
            size = count * 8;
            a = new DoubleAllocator();
        } else if (val instanceof byte[]) {
            count = ((byte[]) val).length;
            size = count;
            a = new ByteAllocator();
        } else if (val instanceof boolean[]) {
            count = ((boolean[]) val).length;
            double bytes = count / 8.;
            size = (int) Math.ceil(bytes);
            a = new BooleanAllocator();
        } else if (val instanceof int[]) {
            count = ((int[]) val).length;
            size = count * 4;
            a = new IntAllocator();
        } else if (val instanceof short[]) {
            count = ((short[]) val).length;
            size = count * 2;
            a = new ShortAllocator();
        } else if (val instanceof long[]) {
            count = ((long[]) val).length;
            size = count * 8;
            a = new LongAllocator();
        } else if (val instanceof Timestamp[]) {
            count = ((Timestamp[]) val).length;
            size = count * 8;
            a = new TimestampAllocator();
        } else if (val instanceof c.Timespan[]) {
            count = ((c.Timespan[]) val).length;
            size = count * 8;
            a = new TimespanAllocator();
        } else if (val instanceof c.Minute[]) {
            count = ((c.Minute[]) val).length;
            size = count * 8;
            a = new MinuteAllocator();
        } else if (val instanceof String[]) {
            count = ((String[]) val).length;
            size = 0;
            a = new StringAllocator();
            d = DataType.STRING;
        } else if (val instanceof Time[]) {
            count = ((Time[]) val).length;
            size = count * 4;
            a = new TimeAllocator();
        } else if (val instanceof Date[]) {
            count = ((Date[]) val).length;
            size = count * 8;
            a = new DateAllocator();
        } else if (val instanceof Object[]) {
            if (((Object[]) val)[0] instanceof char[]) {
                count = ((Object[]) val).length;
                size = 0;
                a = new StringAllocator();
                d = DataType.STRING;
            } else {
                for (Object o : (Object[]) val) {
                    CountContainer innerTriple = getCountLength(name, o);
                    count += 1;
                    size += innerTriple.size;
                    a = innerTriple.allocator;
                }
                d = DataType.ARRAY;
            }
        } else {
            throw new UnsupportedOperationException(name);
        }
        return new CountContainer(size, count, a, d);
    }

    private int getStringSize(Object val) throws UnsupportedEncodingException {
        int size = 0;
        if (val instanceof String[]) {
            for (String v : ((String[]) val)) {
                size += v.getBytes("UTF-8").length;
            }
            return size;
        } else if (val instanceof Object[]) {
            Object[] o = (Object[]) val;
            if (o.length == 0) {
                return 0;
            }
            if (o[0] instanceof char[]) {
                for (Object c : o) {
                    size += new String((char[]) c).getBytes("UTF-8").length;
                }
                return size;
            }
        }
        throw new UnsupportedEncodingException();
    }

    private UserBitShared.SerializedField getSerializedNulls(int count) {
        UserBitShared.SerializedField.Builder builder = UserBitShared.SerializedField.newBuilder();
        builder.setValueCount(count);
        double bytes = count / 8.;
        count = (int) Math.ceil(bytes);
        builder.setBufferLength(count);
        return builder.build();
    }

    private UserBitShared.SerializedField getSerializedValue(int count, int size, String name) {
        UserBitShared.SerializedField.Builder builder = UserBitShared.SerializedField.newBuilder();
        SchemaPath field = fields.get(name.replace("xx_xx", "$"));
        builder.setNamePart(field.getAsNamePart());
        builder.setBufferLength(size);
        builder.setValueCount(count);
        return builder.build();
    }

    @SuppressWarnings("Duplicates")
    private int getFlipPrimitive(String name, Object val, CountContainer countSize, BufferAllocator allocator) throws IOException {
        int dataSize = 0;
        UserBitShared.SerializedField serializedField = getSerializedField(name, countSize.size, countSize.count, countSize.dataType, val);
        int dataLength;
        int offsetLength = 0;
        int bitsLength;
        if (countSize.dataType == DataType.STRING) {
            offsetLength = serializedField.getChild(1).getChild(0).getBufferLength();
            dataLength = serializedField.getChild(1).getBufferLength() - offsetLength;
            bitsLength = serializedField.getChild(0).getBufferLength();
        } else {
            dataLength = serializedField.getChild(1).getBufferLength();
            bitsLength = serializedField.getChild(0).getBufferLength();
        }
        dataSize = serializedField.getValueCount();
        NullCheck check = NullCheckBuilder.build(name, val, serializedField);
//        System.out.println(name);
        try (ArrowBuf buf = allocator.buffer(dataLength + bitsLength + offsetLength)) {
            ArrowBuf outBuf = buf.slice(bitsLength, dataLength + offsetLength).writerIndex(0);
            countSize.allocator.get(val, dataLength + offsetLength, dataSize, outBuf, null);
            getNulls(buf, check, dataLength, bitsLength, offsetLength);
            TypeHelper.load(vectors.get(name).get(), serializedField, buf);
        }
        return dataSize;
    }

    @SuppressWarnings("Duplicates")
    private int getFlipArray(String name, Object val, CountContainer countSize, BufferAllocator allocator) throws IOException {
        int dataSize = 0;
        UserBitShared.SerializedField serializedField = getSerializedField(name, countSize.size, countSize.count, countSize.dataType, val);

        int offsetLength = serializedField.getChild(0).getBufferLength();
        int bitsLength = serializedField.getChild(1).getBufferLength();
        UserBitShared.SerializedField innerSerializedField = serializedField.getChild(2);
        int innerOffestLength = 0;
        int dataLength;
        int innerBitsLength;
        int innerDataLength;
        if (innerSerializedField.getChild(1).getChildCount() > 0) {
            innerOffestLength = innerSerializedField.getChild(1).getChild(0).getBufferLength();
            innerDataLength = innerSerializedField.getChild(1).getBufferLength() - offsetLength;
            innerBitsLength = innerSerializedField.getChild(0).getBufferLength();
        } else {
            innerDataLength = innerSerializedField.getChild(1).getBufferLength();
            innerBitsLength = innerSerializedField.getChild(0).getBufferLength();
        }

        dataLength = innerOffestLength + innerBitsLength + innerDataLength;
        dataSize = serializedField.getValueCount();
        NullCheck check = NullCheckBuilder.build(name, val, serializedField);
        NullCheck checkInner = ((ArrayNullCheck) check).getInner();


        try (ArrowBuf buf = allocator.buffer(dataLength + bitsLength + offsetLength)) {
            ArrowBuf outBuf = buf.slice(bitsLength + offsetLength + innerBitsLength, dataLength).writerIndex(0);
            ArrowBuf offsets = buf.slice(0, offsetLength).writerIndex(0);
            countSize.allocator.get(val, dataLength + offsetLength, dataSize, outBuf, offsets);
            ArrayNullCheck.OffsetBuff offsetBuff = new ArrayNullCheck.OffsetBuff(buf.slice(0, offsetLength), countSize.allocator.offset());
            ((ArrayNullCheck) check).setOffset(offsetBuff);
            getNulls(buf.slice(offsetLength, bitsLength + dataLength).writerIndex(0), check, dataLength - innerOffestLength - innerBitsLength, bitsLength + innerOffestLength + innerBitsLength, 0);
            getNulls(buf.slice(offsetLength + bitsLength, dataLength).writerIndex(0), checkInner, innerDataLength, innerBitsLength, innerOffestLength);
            TypeHelper.load(vectors.get(name).get(), serializedField, buf);
        }
        return dataSize;
    }

    private int getFlip(c.Flip resultSet, BufferAllocator allocator) throws IOException {
        int dataSize = 0;
        for (int i = 0; i < resultSet.y.length; i++) {
            String name = resultSet.x[i].replace("xx_xx", "$");
            Object val = resultSet.y[i];
            CountContainer countSize = getCountLength(name, val);
            if (countSize.dataType == DataType.ARRAY) {
                dataSize = getFlipArray(name, val, countSize, allocator);
            } else {
                dataSize = getFlipPrimitive(name, val, countSize, allocator);
            }
        }
        return dataSize;
    }

    private UserBitShared.SerializedField getSerializedField(String name, int size, int count, DataType dataType, Object val) {
        UserBitShared.SerializedField bits = getSerializedNulls(count);
        UserBitShared.SerializedField vals = null;
        switch (dataType) {
            case STRING:
                vals = getStringSerializedValue(name, size, count, val);
                break;
            case PRIMITIVE:
                vals = getSerializedValue(count, size, name);
                break;
            case ARRAY:
                int innerCount = 0;
                int innerSize = 0;
                DataType d = null;
                UserBitShared.SerializedField offsets = getArraySerializedOffsets(count);
                for (Object o : (Object[]) val) {
                    CountContainer innerTriple = getCountLength(name, o);
                    innerCount += innerTriple.count;
                    innerSize += innerTriple.size;
                    d = innerTriple.dataType;
                }
                vals = getSerializedField(name, innerSize, innerCount, d, val); //todo this wont handle strings well!!
                return UserBitShared.SerializedField.newBuilder()
                        .addChild(offsets)
                        .addChild(bits)
                        .addChild(vals)
                        .setValueCount(count)
                        .build();
            default:
                throw new UnsupportedOperationException(dataType + " not in switch statement");
        }
        return UserBitShared.SerializedField.newBuilder()
                .addChild(bits)
                .addChild(vals)
                .setValueCount(vals.getValueCount())
                .setBufferLength(vals.getBufferLength() + bits.getBufferLength())
                .build();
    }

    private UserBitShared.SerializedField getArraySerializedOffsets(int count) {
        UserBitShared.SerializedField.Builder offsetsBuilder = UserBitShared.SerializedField.newBuilder();
        offsetsBuilder.setBufferLength(count * 4 + 4);
        offsetsBuilder.setValueCount(count + 1);
        UserBitShared.SerializedField offset = offsetsBuilder.build();
        return offset;
    }

    private UserBitShared.SerializedField getStringSerializedValue(String name, int size, int count, Object val) {
        UserBitShared.SerializedField.Builder offsetsBuilder = UserBitShared.SerializedField.newBuilder();
        offsetsBuilder.setBufferLength(count * 4 + 4);
        offsetsBuilder.setValueCount(count + 1);
        UserBitShared.SerializedField offset = offsetsBuilder.build();
        try {
            size = getStringSize(val);
        } catch (UnsupportedEncodingException e) {
            throw Throwables.propagate(e);
        }
        UserBitShared.SerializedField.Builder builder = UserBitShared.SerializedField.newBuilder();
        SchemaPath field = fields.get(name);
        builder.setNamePart(field.getAsNamePart());
        builder.setBufferLength(size + (count + 1) * 4);
        builder.setValueCount(count);
        builder.addChild(offset);
        return builder.build();
    }

    private void getNulls(ArrowBuf in, NullCheck check, int dataLength, int dataSize, int offsetLength) throws IOException {
        ArrowBufInputStream ab = ArrowBufInputStream.getStream(dataSize, dataLength + dataSize + offsetLength, in);
        ArrowBuf buf = in.slice(0, dataSize).writerIndex(0);
        byte value = 0;
        int i = 0;
        while (ab.available() > 0) {
            boolean isNull = check.check(ab);
            value += isNull ? (1 << i) : 0;
            i++;
            if (i == 8) {
                buf.writeByte(value);
                value = 0;
                i = 0;
            }
        }
        if (i > 0) {
            buf.writeByte(value);
        }
    }

    private int getCurrent(BufferAllocator bufferAllocator) throws ArrayIndexOutOfBoundsException, IOException {
        if (resultSet instanceof c.Flip) {
            int i = getFlip((c.Flip) resultSet, bufferAllocator);
            return i;
        }
        if (resultSet instanceof c.Dict) {
            int i = getDict((c.Dict) resultSet, bufferAllocator);
            return i;
        }
        throw new IOException("cant deal with " + resultSet.getClass().toString());
    }

    private int getDict(c.Dict resultSet, BufferAllocator bufferAllocator) throws IOException {
        c.Flip keys = (c.Flip) resultSet.x;
        c.Flip values = (c.Flip) resultSet.y;
        int i = getFlip(keys, bufferAllocator);
        int ii = getFlip(values, bufferAllocator);
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
        } catch (c.KException | IOException e) {
            LOGGER.error("Unable to capture count of table .temp._" + uuid);
            return 0;
        }
        return 0;
    }

    private enum DataType {
        ARRAY,
        STRING,
        PRIMITIVE
    }

    enum ReadState {
        END_OF_STREAM, WRITE_SUCCEED, ERROR
    }

    private static final class CountContainer {
        private final int size;
        private final int count;
        private final Allocator allocator;
        private final DataType dataType;

        private CountContainer(int size, int count, Allocator allocator, DataType dataType) {
            this.size = size;
            this.count = count;
            this.allocator = allocator;
            this.dataType = dataType;
        }

        public int getSize() {
            return size;
        }

        public int getCount() {
            return count;
        }

        public Allocator getAllocator() {
            return allocator;
        }

        public DataType getDataType() {
            return dataType;
        }
    }
}
