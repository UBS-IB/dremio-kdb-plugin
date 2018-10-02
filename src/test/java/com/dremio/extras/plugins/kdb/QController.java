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

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import com.dremio.BaseTestQuery;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.store.CatalogService;
import com.dremio.sabot.rpc.user.QueryDataBatch;
import com.dremio.service.namespace.source.proto.SourceConfig;

/**
 * build q executable and load w/ initial data
 */
public class QController extends BaseTestQuery {

    private static QController self;
    private static KdbStoragePluginConfig storagePluginConfig;
    private Process process;

    @BeforeClass
    public static void setupKdbTestCluster() throws Exception {
        self = new QController();
        self.start();

        storagePluginConfig = new KdbStoragePluginConfig(null, null, "localhost", "1234", "0");
        SourceConfig sc = new SourceConfig();
        sc.setName("kdb");
        sc.setConnectionConf(storagePluginConfig);
        sc.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);
        ((CatalogServiceImpl) getSabotContext().getCatalogService()).getSystemUserCatalog().createSource(sc);

    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        self.stop();
    }

    public void start() throws IOException, c.KException {
        String os = (System.getProperty("os.name").contains("indow")) ? "w32" : "l32";
        URL qExecutable = QController.class.getClassLoader().getResource("q/" + os + "/q" + (("w32".equals(os)) ? ".exe" : ""));
        URL qHome = QController.class.getClassLoader().getResource("q");

        ProcessBuilder builder = new ProcessBuilder("dir");
        Map<String, String> env = builder.environment();
        if ("w32".equals(os)) {
            env.put("QHOME", qHome.getFile().replace("/", "\\").replaceFirst("\\\\", ""));
        } else {
            env.put("QHOME", qHome.getFile());
        }
        String[] envStr = new String[env.size()];
        int i = 0;
        for (Map.Entry<String, String> kv : env.entrySet()) {
            envStr[i++] = (kv.getKey() + "=" + kv.getValue());
        }
        String[] cmd;
        if ("w32".equals(os)) {
            cmd = new String[]{qExecutable.getFile().replace("/", "\\").replaceFirst("\\\\", ""), "-p", "1234"};
        } else {
            cmd = new String[]{qExecutable.getFile(), "-p", "1234"};
        }
        Process p = Runtime.getRuntime().exec(cmd, envStr);
        this.process = p;
        init();
    }

    public long count(String query) {
        String finalQuery = "count " + query;
        try {
            c c = new c("localhost", 1234);
            Object x = c.k(finalQuery);
            if (x instanceof Long) {
                return (Long) x;
            }
            return 0;
        } catch (IOException e) {
            return -1;
        } catch (c.KException e) {
            return -1;
        }
    }

    private void init() throws IOException, c.KException {
        c c = new c("localhost", 1234);
        c.k("\\l trade.q");
        c.k("\\l sp.q");
        c.k("md:`:fxkdbldns4.ldn.swissbank.com:19051:kdbprod:kdbprod \"select[1000] from CURRENEX_stm\"");
//    c.k("mor:`:fxkdbldns1.ldn.swissbank.com:7061:kdbprod:kdbprod \"select[100] from mortrade\"");
    }

    public void stop() {
        process.destroy();
    }


    protected List<QueryDataBatch> runKdbSQLWithResults(String sql) throws Exception {
        System.out.println("Running query:\n" + sql);
        return testSqlWithResults(sql);
    }

    protected void runKdbSQLVerifyCount(String sql, int expectedRowCount) throws Exception {
        List<QueryDataBatch> results = runKdbSQLWithResults(sql);
        printResultAndVerifyRowCount(results, expectedRowCount);
    }

    private void printResultAndVerifyRowCount(List<QueryDataBatch> results, int expectedRowCount) throws SchemaChangeException {
        int rowCount = printResult(results);
        if (expectedRowCount != -1) {
            Assert.assertEquals(expectedRowCount, rowCount);
        }
    }

}
