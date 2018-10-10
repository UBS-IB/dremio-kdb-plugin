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
import java.util.List;
import java.util.Map;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

/**
 * read batches off the kdb ipc channel and into arrow arrays
 */
public class KdbRecordReader extends AbstractRecordReader {


    private static final Logger LOGGER = LoggerFactory.getLogger(KdbRecordReader.class);
    private static final int STREAM_COUNT_BREAK_MULTIPLIER = 3;
    private static final String TERMINATED_EARLY = "\"terminated_early\": true";
    private static final String TIMED_OUT = "\"timed_out\": true";
    private final String query;

    private final KdbConnection connection;
    private final OperatorStats stats;
    private final int batchSize;
    private BatchSchema schema;
    private BufferAllocator allocator;
    private long totalSize = Integer.MAX_VALUE;
    private long totalCount;
    private KdbReader kdbReader;
    private State state = State.INIT;

    public KdbRecordReader(
            OperatorContext context, List<SchemaPath> columns, String sql, KdbSchema kdbSchema, int batchSize, BatchSchema schema) {
        super(context, columns);
        this.query = sql;
        connection = kdbSchema.getKdb();
        this.stats = context == null ? null : context.getStats();
        this.batchSize = batchSize;
        this.schema = schema;

    }

    @Override
    public void setup(OutputMutator output) throws ExecutionSetupException {
        allocator = context.getAllocator();

        Map<String, VectorGetter> vectors = Maps.newHashMap();
        Map<String, SchemaPath> fields = Maps.newHashMap();
        for (SchemaPath column : getColumns()) {
            String name = column.getAsNamePart().getName();
            Field f = schema.findField(name);
            vectors.put(name, new VectorGetter(output, f));
            fields.put(name, column);

        }
        kdbReader = new KdbReader(query, connection, vectors, fields);
    }

    @Override
    protected boolean supportsSkipAllQuery() {
        return true;
    }

    private void getFirstPage() {
        assert state == State.INIT;
        int searchSize = this.batchSize;
        int fetch = -1;
        if (fetch >= 0 && fetch < searchSize) {
            searchSize = fetch;
        }

        kdbReader.setBatchSize(searchSize);
        getNextPage();
        totalSize = kdbReader.getTotalSize();
        state = State.READ;
    }

    private void getNextPage() {
        try {
            if (stats != null) {
                stats.startWait();
            }
            kdbReader.nextQuery();
        } catch (IOException e) {
            throw UserException.dataReadError(e).message("Failure when initating Kdb query.").addContext("Query", query).build(LOGGER);
        } catch (c.KException e) {
            throw UserException.dataReadError(e).message("Failure when initating Kdb query.").addContext("Query", query).build(LOGGER);
        } finally {
            if (stats != null) {
                stats.stopWait();
            }
        }
    }

    @Override
    public int next() {
        if (state == State.DEPLETED || state == State.CLOSED) {
            return 0;
        }

        if (state == State.INIT) {
            getFirstPage();
        }

        assert state == State.READ;

        int pageCount = 0;
        int count = 0;
        try {
            while (count < numRowsPerBatch) {
                if (state == State.DEPLETED) {
                    break;
                }

                Pair<KdbReader.ReadState, Integer> pair = kdbReader.write(allocator);
                KdbReader.ReadState readState = pair.getLeft();
                if (readState == KdbReader.ReadState.WRITE_SUCCEED) {
                    count += pair.getRight();
                    totalCount += pair.getRight();
                    continue;
                }

                // if we receive the records we were told we'd receive, we will should stop reading.
                if (totalCount == totalSize || readState == KdbReader.ReadState.END_OF_STREAM) {
                    state = State.DEPLETED;
                    break;
                }

                if (readState != KdbReader.ReadState.ERROR) {
                    getNextPage();
                    pageCount++;

                    boolean badStreamBreak = pageCount > STREAM_COUNT_BREAK_MULTIPLIER * numRowsPerBatch / (1.0 * -1.0) && pageCount > 5;

                    if (!badStreamBreak) {
                        continue;
                    }
                }

                // we didn't get the records we expected within a reasonable amount of time.
                final String latest = kdbReader.getError();
                final boolean timedOut = latest.contains(TIMED_OUT);
                final boolean terminatedEarly = latest.contains(TERMINATED_EARLY);
                final UserException.Builder builder = UserException.dataReadError();
                if (timedOut) {
                    builder.message("Kdb failed with scroll timed out.");

                } else if (terminatedEarly) {
                    builder.message("Kdb failed with early termination.");
                } else {
                    builder.message("Kdb query terminated as Dremio didn't receive as many results as expected.");
                }

                builder.addContext("Query", this.query);
                builder.addContext("Final Response", latest);
                throw builder.build(LOGGER);

            }

        } catch (Exception e) {
            throw Throwables.propagate(e);
        }

        return count;
    }

    @Override
    public synchronized void close() throws Exception {
        if (state == State.CLOSED) {
            return;
        }

        try {
            kdbReader.delete();
        } catch (Exception e) {
            LOGGER.warn("Failure while deleting table: " + kdbReader.getUuid());
        } finally {
            state = State.CLOSED;
        }
    }

    //todo beign very cavalier about how i write this pager....test me!

    enum State {INIT, READ, DEPLETED, CLOSED}

    /**
     * Helper method to cache valuevector
     */
    public static final class VectorGetter implements Provider<ValueVector> {
        private final OutputMutator output;
        private final Field f;
        private ValueVector vector = null;

        public VectorGetter(OutputMutator output, Field f) {
            this.output = output;
            this.f = f;
        }

        @Override
        public ValueVector get() {
            if (vector == null) {
                vector = (output.addField(f, (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(f)));
            }
            return vector;
        }
    }

}
