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

import org.apache.commons.lang3.tuple.Triple;

import com.google.common.collect.Maps;

/**
 * connection helper class for Kdb. Hide the c object
 */
public class KdbConnection {
    private final String hostname;
    private final int port;
    private final String username;
    private final String password;
    private final Map<String, Triple<String[], char[], String[]>> schemas = Maps.newHashMap();
    private c conn;
    private String[] tables;

    public KdbConnection(String hostname, int port, String username, String password) {
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    private c getConn() {
        if (conn == null) {
            try {
                if (username != null) {
                    conn = new c(hostname, port, username + ":" + password);
                } else {
                    conn = new c(hostname, port);
                }
            } catch (c.KException | IOException e) {
                throw new RuntimeException(e);
            }
        }
        return conn;
    }

    public String[] getTables() {
        if (tables == null) {
            c connection = getConn();
            String[] foundTables = new String[0];
            try {
                foundTables = (String[]) connection.k("tables[]");
            } catch (c.KException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.tables = foundTables;
        }
        return tables;
    }

    public Triple<String[], char[], String[]> getSchema(String table) {
        if (!schemas.containsKey(table)) {
            c connection = getConn();
            try {
                Object schema = connection.k("meta " + table);
                /*
                The result of meta command looks like:

                c           t       f       a
                "market"    s       ""      ""
                "xeUser"    C       ""      ""

                The key column c of the result contains the column names.
                The column t contains a symbol denoting the type char of the column.
                The column f contains the domains of any foreign key or link columns.
                The column a contains any attributes associated with the column.
                 */
                schemas.put(table, convert((c.Dict) schema));
            } catch (c.KException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return schemas.get(table);
    }

    private Triple<String[], char[], String[]> convert(c.Dict meta) {
        c.Flip cols = (c.Flip) meta.x;
        String[] names = (String[]) cols.y[0];
        c.Flip typesMap = (c.Flip) meta.y;
        char[] types = (char[]) typesMap.y[0];
        String[] attrs = (String[]) typesMap.y[2];
        //todo deal w/ partitions etc
        return Triple.of(names, types, attrs);
    }

    public Object select(String source) throws IOException {
        try {
            Object results = getConn().k(source);
            return results;
        } catch (c.KException e) {
            String exception = Errors.ERROR_MAP.getOrDefault(e.getMessage(), e.getMessage());
            try {
                String query = source;
                if (source.contains(".temp.") && source.contains(":(")) {
                    String[] queryPieces = source.split(":", 2);
                    query = queryPieces[1].substring(1, queryPieces[1].length() - 1);
                }
                Object parse = getConn().k("parse \"" + query + "\"");
                exception += (" " + getParseString(parse));
            } catch (Throwable t) {
                exception += " could not get parse tree";
            }
            throw new IOException(exception, e);
        }
    }

    private static String getParseString(Object parse) {
        if (parse instanceof String[]) {
            StringBuilder x = new StringBuilder();
            for (String s : (String[]) parse) {
                x.append(" ").append(s);
            }
            return x.toString() + "\n";
        } else if (parse instanceof Object[]) {
            StringBuilder x = new StringBuilder();
            for (Object o : (Object[]) parse) {
                x.append(getParseString(o));
            }
            return x.toString() + "\n";
        } else if (parse instanceof String) {
            return (String) parse + ", ";
        }
        return parse.toString() + ", ";
    }
}

