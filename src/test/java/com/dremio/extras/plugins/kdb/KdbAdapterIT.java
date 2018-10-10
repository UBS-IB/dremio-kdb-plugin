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
package com.dremio.extras.plugins.kdb;

import java.sql.Date;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Assert;
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
        runKdbSQLVerifyCount("select * from kdb.trade order by sym desc limit 1000", 1000);
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
                + "where sym = 'AAPL' limit 4", 4);
    }

    /**
     * Tests that we don't generate multiple constraints on the same column.
     * Kdb doesn't like it. If there is an '=', it supersedes all other
     * operators.
     */
    @Test
    public void testFilterRedundant() throws Exception {
        runKdbSQLVerifyCount(
                "select * from kdb.trade where sym > 'AAPL' and sym <= 'GOOG' and sym = 'GOOG' limit 100", 100);
    }

    @Test
    public void testFilterRedundantNonsense() throws Exception {
        runKdbSQLVerifyCount(
                "select * from kdb.trade where sym > 'AAPL' and sym < 'GOOG' and sym = 'GOOG' limit 100", 0);
    }

    @Test
    public void testSelectWhere() throws Exception {
        runKdbSQLVerifyCount(
                "select * from kdb.p where p = 'p1'", 1);
    }

    @Test
    public void testInPlan() throws Exception {
        runKdbSQLVerifyCount("select sym, price from kdb.trade\n"
                + "where sym in ('AAPL', 'GOOG') limit 1000", 1000);
    }

    /**
     * Simple query based on the "kdb-zips" model.
     */
    @Test
    public void testZips() throws Exception {
        runKdbSQLVerifyCount("select sym, price from kdb.trade limit 1000", 1000);
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
        runKdbSQLVerifyCount("select count(*) from kdb.trade group by sym order by 1", 15);
    }

    @Test
    public void testGroupByOneColumn() throws Exception {
        runKdbSQLVerifyCount(
                "select sym, count(*) as c from kdb.trade group by sym order by sym ", 15);
    }

    @Test
    public void testGroupByOneColumnReversed() throws Exception {
        // Note extra $project compared to testGroupByOneColumn.
        runKdbSQLVerifyCount(
                "select count(*) as c, sym from kdb.trade group by sym order by sym", 15);

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

    @Test
    public void testGroupByAvgPrice() throws Exception {
        runKdbSQLVerifyCount(
                "select sym, avg(size*price) as c from kdb.trade group by sym order by sym ", 15);

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
        org.junit.Assume.assumeTrue(SystemUtils.IS_OS_LINUX);
        runKdbSQLVerifyCount("select sym, count(distinct \"time\") as cdc from kdb.trade\n"
                + "where sym in ('AAPL','GOOG') group by sym order by sym", 2);
    }

    @Test
    public void testMultiCount() throws Exception {
        runKdbSQLVerifyCount("select sym, count(*) from (select sym, count('time') as cd from kdb.trade\n"
                + "where sym in ('AAPL','GOOG') group by sym) group by sym", 2);
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
        runKdbSQLVerifyCount("select sym, price, 0 as zero from kdb.trade order by sym, price limit 1000", 1000);
    }

    @Test
    public void testFilter() throws Exception {
        runKdbSQLVerifyCount("select sym, size from kdb.trade where sym = 'AAPL' limit 1000", 1000);
    }

    /**
     * KdbDB's predicates are handed (they can only accept literals on the
     * right-hand size) so it's worth testing that we handle them right both
     * ways around.
     */
    @Test
    public void testFilterReversed() throws Exception {
        runKdbSQLVerifyCount("select price, sym from kdb.trade where 10 < size limit 100", 100);
        runKdbSQLVerifyCount("select price, sym from kdb.trade where size > 10 limit 100", 100);
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
        String currentDate = new Date(System.currentTimeMillis()).toString();
        runKdbSQLVerifyCount("select current_date, price from kdb.trade where price > 11.0 limit 1000", 1000);
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
        runKdbSQLVerifyCount("SELECT date_trunc('hour', \"time\") as hr, sym, count(*), sum(bsize), avg(case when asize>50 then asize else 0 end)  " +
                "FROM kdb.quote " +
                "group by date_trunc('hour', \"time\"), sym", 105);
    }

    @Test
    public void testGroupBySimpleCase() throws Exception {
        runKdbSQLVerifyCount("SELECT sym, sum(case when asize>50 then asize else 0 end)  " +
                "FROM kdb.quote " +
                "group by sym", 15);
    }

    @Test
    public void testSimpleCase() throws Exception {
        runKdbSQLVerifyCount("SELECT case when asize<2 then asize else 0 end " +
                "FROM kdb.quote limit 1000", 1000);
    }

    @Test
    public void testGroupByDateSimple() throws Exception {
        runKdbSQLVerifyCount("SELECT date_trunc('hour', \"time\") as hr, count(*)" +
                "FROM kdb.quote " +
                "group by date_trunc('hour', \"time\") limit 100", 7);
    }

    // KDB uses '*' instead of '%' and '?' instead of "_" for pattern matching.
    @Test
    public void testLikeOnVarcharColumn() throws Exception {
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE YYY LiKe '%2461' ", 0);
    }

    @Test
    public void testLikeOnSymbolColumn() throws Exception {
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX LikE '277c5%' ", 177);
    }

    @Test
    public void testLikeWithMultipleWildcardsOnVarcharColumn() throws Exception {
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE YYY LiKe '%246%' ", 2);
    }

    @Test
    public void testLikeWithMultipleWildcardsOnSymbolColumn() throws Exception {
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX LikE '%c60267da%' ", 0);
    }

    @Test
    public void testLikeWithUnderscoreWildcardsOnVarcharColumn() throws Exception {
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE YYY LiKe '%5d4_' ", 0);
    }

    @Test
    public void testLikeWithUnderscoreWildcardsOnSymbolColumn() throws Exception {
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX LikE '_77c5%' ", 0);
    }

    @Test
    public void testEqualsWithWildcardsInLiteralOnVarcharColumn() throws Exception {
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE YYY = '%080_' ", 0);
    }

    @Test
    public void testEqualsWithWildcardsInLiteralOnSymbolColumn() throws Exception {
        // this will get translated into
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX like '77c5%' ", 0);
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX like '%c5' ", 0);
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX like '%77c%' ", 177);
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX like '277c5_' ", 0);
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX like '_277c5' ", 0);
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX like '_277c5_' ", 0);
    }

    // KDB treats Symbols differently then varchars (symbol is 's' while char array is 'c')
    @Test
    public void testEqualsOnVarcharColumn() throws Exception {
        // kdb needs to turn this into: select from daptrades_strings where YYY like "PCC382461"
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE YYY = '80807bd9d4a6c60267daa2f4378fb5d4' ", 6);
    }

    @Test
    public void testEqualsOnSymbolColumn() throws Exception {
        // kdb needs to turn this into: select from daptrades_strings where XXX = `Xmo_Emcredit_ALL_Agency
        runKdbSQLVerifyCount("SELECT XXX, YYY FROM kdb.daptrades_strings WHERE XXX = '277c5fb1c7dde07c7e103427fc37e0be' ", 177);
    }

    @Test
    public void selectArrayString() throws Exception {
        runKdbSQLVerifyCount("SELECT * FROM kdb.daptrade_problematic_array_columns LIMIT 50", 50);
    }

    @Test
    public void queryOnHdb() throws Exception {
        runKdbSQLVerifyCount("SELECT * FROM kdb.nbbo LIMIT 50", 50);
    }

    @Test
    public void queryOnHdbFail() {
        try {
            runKdbSQLVerifyCount("SELECT * FROM kdb.nbbo limit 50", 50);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("No limit clause and not filtering"));
        }
    }

    @Test
    public void queryOnHdbFilter() throws Exception {
        runKdbSQLVerifyCount("    SELECT *\n" +
                "    FROM kdb.nbbo\n" +
                "    where \"date\"=to_date('2013-05-01','YYYY-mm-dd')\n" +
                "    limit 10", 10);
    }

    @Test
    public void queryOnHdbFilterPlusDay() throws Exception {
        runKdbSQLVerifyCount("    SELECT *,date_add(\"date\",1)\n" +
                "    FROM kdb.nbbo\n" +
                "    limit 10", 10);
        runKdbSQLVerifyCount("    SELECT *,date_sub(\"date\",1)\n" +
                "    FROM kdb.nbbo\n" +
                "    limit 10", 10);
    }

    @Test
    public void testFunctions() throws Exception {
        for (String operator : new String[]{"abs", "acos", "atan", "asin", "cos", "sin", "tan", "exp", "sqrt", "log", "log10"}) {
            runKdbSQLVerifyCount("select sum(" + operator + "(price)), sym from kdb.depth group by sym", 15);
        }
    }

    @Test
    public void testAggs() throws Exception {
        runKdbSQLVerifyCount("select \"corr\"(price, price), sym from kdb.depth group by sym", 15);
        for (String operator : new String[]{"sum", "avg", "min", "max", "stddev", "stddev_pop", "var", "var_pop"}) {
            runKdbSQLVerifyCount("select " + operator + "(price), sym from kdb.depth group by sym", 15);
        }
    }
}

// End KdbAdapterIT.java
