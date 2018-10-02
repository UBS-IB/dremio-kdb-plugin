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
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;

/**
 * Schema for kdb instance
 */
public class KdbSchema {
    private final KdbConnection kdb;

    public KdbSchema(String host, int port, String username, String password) {
        try {
            this.kdb = new KdbConnection(host, port, username, password);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public Set<String> getTableNames() {
        return this.getTableMap().keySet();
    }

    public KdbTable getTable(String name) {
        return this.getTableMap().get(name);
    }

    private Map<String, KdbTable> getTableMap() {
        final ImmutableMap.Builder<String, KdbTable> builder = ImmutableMap.builder();
        for (String collectionName : kdb.getTables()) {
            builder.put(collectionName, new KdbTable(collectionName, kdb, null));
        }
        return builder.build();
    }

    public double getVersion() throws IOException, c.KException {
        Object x = kdb.select(".z.K");
        return (double) x;
    }

    public KdbConnection getKdb() {
        return kdb;
    }
}
