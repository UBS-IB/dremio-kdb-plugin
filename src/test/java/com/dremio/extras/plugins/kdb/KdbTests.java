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
package com.dremio.extras.plugins.kdb;

import org.junit.Ignore;
import org.junit.Test;

/**
 * tests imported from calcite
 */
public class KdbTests extends QController {

    @Test
    public void testCount() throws Exception {
        runKdbSQLVerifyCount("select qty from (select * from kdb.sp\n"
                + "where qty BETWEEN 100 AND 300)", 9);
    }

    @Test
    public void testSub() throws Exception {
        runKdbSQLVerifyCount("select qty as x from kdb.sp\n"
                        + "where qty BETWEEN 100 AND 300"
                , 9);
    }

    @Test
    public void testSrt() throws Exception {
        runKdbSQLVerifyCount("select s, sum(qty) from kdb.sp where qty > 100\n"
                        + " group by s order by s limit 4"
                , 4);
    }

    @Test
    public void testVector() throws Exception {
        runKdbSQLVerifyCount("select * from kdb.md where sym in ('EURUSD','USDJPY') and bidCount = 5 ", 100);
    }

    @Test
    public void testChar() throws Exception {
        runKdbSQLVerifyCount("select * from kdb.mor", 100);
    }

    @Test
    @Ignore
    public void testEmptySum() throws Exception {
        runKdbSQLVerifyCount("select count(sym) from kdb.mor", 1);
    }

    @Test
    @Ignore
    public void testDistinct() throws Exception {
        runKdbSQLVerifyCount("select distinct sym from kdb.mor", 100);
    }
}
