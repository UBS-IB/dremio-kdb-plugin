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

import java.sql.Date;

import org.junit.Ignore;
import org.junit.Test;


/**
 * Tests for the {@code org.apache.calcite.adapter.kdb} package.
 *
 * <p>Before calling this test, you need to populate KdbDB, as follows:
 *
 * <blockquote><code>
 * git clone https://github.com/vlsi/calcite-test-dataset<br>
 * cd calcite-test-dataset<br>
 * mvn install
 * </code></blockquote>
 *
 * <p>This will create a virtual machine with KdbDB and "zips" and "foodmart"
 * data sets.
 */
public class KdbAdapterIT extends QController {

    @Test
    public void testSort() throws Exception {
        runKdbSQLVerifyCount("select * from kdb.trade order by sym desc", 3);
    }

    @Test
    public void testSortLimit() throws Exception {
        runKdbSQLVerifyCount("select p, s from kdb.sp\n"
                + "order by s, p offset 2 rows fetch next 3 rows only", 3);
    }

    @Test
    public void testOffsetLimit() throws Exception {
        runKdbSQLVerifyCount("select p, s from kdb.sp\n"
                + "offset 2 fetch next 3 rows only", 3);
    }

    @Test
    public void testLimit() throws Exception {
        runKdbSQLVerifyCount("select s, p from kdb.sp\n"
                + "fetch next 3 rows only", 3);
    }

    @Test
    public void testFilterSortDesc() throws Exception {
        runKdbSQLVerifyCount("select * from kdb.sp\n"
                + "where qty BETWEEN 100 AND 300\n"
                + "order by s desc, qty limit 4", 4);
    }

    @Test
    public void testFilterOrderSortDesc() throws Exception {
        runKdbSQLVerifyCount("select * from kdb.sp\n"
                + "where qty BETWEEN 100 AND 300 and s='s1'\n"
                + "order by s desc, qty limit 4", 4);
    }


    @Test
    public void testUnionPlan() throws Exception {
        runKdbSQLVerifyCount("select * from kdb.trade\n"
                + "union all\n"
                + "select * from kdb.trade limit 2", 2);
    }

    @Test
    public void testFilterUnionPlan() throws Exception {
        runKdbSQLVerifyCount("select * from (\n"
                + "  select * from kdb.trade\n"
                + "  union all\n"
                + "  select * from kdb.trade)\n"
                + "where sym = 'a'", 4);
    }

    /**
     * Tests that we don't generate multiple constraints on the same column.
     * Kdb doesn't like it. If there is an '=', it supersedes all other
     * operators.
     */
    @Test
    public void testFilterRedundant() throws Exception {
        runKdbSQLVerifyCount(
                "select * from kdb.trade where sym > 'a' and sym <= 'b' and sym = 'b'", 1);
    }

    @Test
    public void testFilterRedundantNonsense() throws Exception {
        runKdbSQLVerifyCount(
                "select * from kdb.trade where sym > 'a' and sym < 'b' and sym = 'b'", 0);
    }

    @Test
    public void testSelectWhere() throws Exception {
        runKdbSQLVerifyCount(
                "select * from kdb.p where p = 'p1'", 1);
    }

    @Test
    public void testInPlan() throws Exception {
        runKdbSQLVerifyCount("select sym, price from kdb.trade\n"
                + "where sym in ('a', 'b')", 3);
    }

    /**
     * Simple query based on the "kdb-zips" model.
     */
    @Test
    public void testZips() throws Exception {
        runKdbSQLVerifyCount("select sym, price from kdb.trade", 3);
    }

    @Test
    public void testCountGroupByEmpty() throws Exception {
        runKdbSQLVerifyCount("select count(*) from kdb.trade", 1);
    }

    @Test
    public void testCountGroupByEmptyMultiplyBy2() throws Exception {
        //todo
        runKdbSQLVerifyCount("select count(*)*2 from kdb.trade", 1);
    }

    @Test
    public void testGroupByOneColumnNotProjected() throws Exception {
        runKdbSQLVerifyCount("select count(*) from kdb.trade group by sym order by 1 limit 2", 2);
    }

    @Test
    public void testGroupByOneColumn() throws Exception {
        runKdbSQLVerifyCount(
                "select sym, count(*) as c from kdb.trade group by sym order by sym limit 2", 2);
    }

    @Test
    public void testGroupByOneColumnReversed() throws Exception {
        // Note extra $project compared to testGroupByOneColumn.
        runKdbSQLVerifyCount(
                "select count(*) as c, sym from kdb.trade group by sym order by sym limit 2", 2);

    }

    @Test
    public void testGroupByAvg() throws Exception {
        runKdbSQLVerifyCount(
                "select p, avg(qty) as a from kdb.sp group by p order by p limit 2", 2);
    }

    @Test
    public void testGroupByAvgSumCount() throws Exception {
        runKdbSQLVerifyCount(
                "select p, avg(qty) as a, sum(qty) as b, count(qty) as c from kdb.sp group by p order by p limit 2", 2);

    }

    //todo We do not do this correctly! currently we generate 2x selects when only one is needed
    @Test
    public void testGroupByAvgPrice() throws Exception {
        runKdbSQLVerifyCount(
                "select sym, avg(size*price) as c from kdb.trade group by sym order by sym limit 2", 2);

    }


    @Ignore("have to implement having")
    @Test
    public void testGroupByHaving() throws Exception {
        runKdbSQLVerifyCount("select sym, count(*) as c from kdb.trade\n"
                + "group by sym having count(*) > 1 order by sym", 3);
    }

    @Ignore("have to implement having")
    @Test
    public void testGroupByHaving2() throws Exception {
        runKdbSQLVerifyCount("select sym, count(*) as c from kdb.trade\n"
                + "group by state having sum(size) > 120", 4);
    }

    @Test
    public void testGroupByMinMaxSum() throws Exception {
        runKdbSQLVerifyCount("select count(*) as c, p,\n"
                + " min(qty) as min_pop, max(qty) as max_pop, sum(qty) as sum_pop\n"
                + "from kdb.sp group by p order by p limit 2", 2);
    }

    @Test
    public void testGroupComposite() throws Exception {
        runKdbSQLVerifyCount("select count(*) as c, s, p from kdb.sp\n"
                + "group by s, p order by c desc limit 2", 2);
    }

    @Test
    public void testDistinctCount() throws Exception {
        runKdbSQLVerifyCount("select sym, count(distinct \"time\") as cdc from kdb.trade\n"
                + "where sym in ('a','b') group by sym order by sym", 2);
    }

    @Test
    public void testMultiCount() throws Exception {
        runKdbSQLVerifyCount("select sym, count(*) from (select sym, count('time') as cd from kdb.trade\n"
                + "where sym in ('a','b') group by sym) group by sym", 2);
    }

    @Test
    public void testDistinctCountOrderBy() throws Exception {
        runKdbSQLVerifyCount("select s, count(distinct p) as cdc\n"
                + "from kdb.sp\n"
                + "group by s\n"
                + "order by cdc desc limit 2", 2);
    }

    @Test
    public void testProject() throws Exception {
        runKdbSQLVerifyCount("select sym, price, 0 as zero from kdb.trade order by sym, price", 3);
    }

    @Test
    public void testFilter() throws Exception {
        runKdbSQLVerifyCount("select sym, size from kdb.trade where sym = 'a'", 2);
    }

    /**
     * KdbDB's predicates are handed (they can only accept literals on the
     * right-hand size) so it's worth testing that we handle them right both
     * ways around.
     */
    @Test
    public void testFilterReversed() throws Exception {
        runKdbSQLVerifyCount("select price, sym from kdb.trade where 150 < size", 1);
        runKdbSQLVerifyCount("select price, sym from kdb.trade where size > 150", 1);
    }

    private void checkPredicate(int expected, String q) throws Exception {
        runKdbSQLVerifyCount("select count(*) as c from kdb.sp\n"
                + q, expected);
        runKdbSQLVerifyCount("select * from kdb.sp\n", expected);
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-286">[CALCITE-286]
     * Error casting KdbDB date</a>.
     */
    @Test
    public void testDate() throws Exception {
        // Assumes that you have created the following collection before running
        // this test:
        //
        // $ kdb
        // > use test
        // switched to db test
        // > db.createCollection("datatypes")
        // { "ok" : 1 }
        // > db.datatypes.insert( {
        //     "_id" : ObjectId("53655599e4b0c980df0a8c27"),
        //     "_class" : "com.ericblue.Test",
        //     "date" : ISODate("2012-09-05T07:00:00Z"),
        //     "value" : 1231,
        //     "ownerId" : "531e7789e4b0853ddb861313"
        //   } )
        String currentDate = new Date(System.currentTimeMillis()).toString();
        runKdbSQLVerifyCount("select current_date, price from kdb.trade where price > 11.0 ", 1);
    }

    /**
     * Test case for
     * <a href="https://issues.apache.org/jira/browse/CALCITE-665">[CALCITE-665]
     * ClassCastException in KdbDB adapter</a>.
     */
    @Test
    public void testCountViaInt() throws Exception {
        runKdbSQLVerifyCount("select count(*) from kdb.sp", 1);
    }

    @Test
    public void testGroupByDate() throws Exception {
        runKdbSQLVerifyCount("SELECT date_trunc('hour', \"time\") as hr, sym, count(*), sum(bidCount), avg(case when offerCount>2 then offerCount else 0 end)  " +
                "FROM kdb.md " +
                "group by date_trunc('hour', \"time\"), sym", 25);
    }

    @Test
    public void testGroupBySimpleCase() throws Exception {
        runKdbSQLVerifyCount("SELECT sym, sum(case when offerCount>2 then offerCount else 0 end)  " +
                "FROM kdb.md " +
                "group by sym", 25);
    }

    @Test
    public void testSimpleCase() throws Exception {
        runKdbSQLVerifyCount("SELECT case when offerCount<2 then offerCount else 0 end " +
                "FROM kdb.md ", 1000);
    }

    @Test
    public void testGroupByDateSimple() throws Exception {
        runKdbSQLVerifyCount("SELECT date_trunc('hour', \"time\") as hr, count(*)" +
                "FROM kdb.md " +
                "group by date_trunc('hour', \"time\")", 1);
    }

    @Test
    public void testArrayOfStrings() throws Exception {
        runKdbSQLVerifyCount("SELECT waiverFlag FROM kdb.daptrade", 100);
    }
}

// End KdbAdapterIT.java
