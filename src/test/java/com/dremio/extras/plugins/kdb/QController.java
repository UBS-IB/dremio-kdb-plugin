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

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

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

        storagePluginConfig = new KdbStoragePluginConfig();
        storagePluginConfig.host = "localhost";
        storagePluginConfig.port = 1234;
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

    public void start() throws IOException, c.KException, URISyntaxException {
        String os = (System.getProperty("os.name").contains("indow")) ? "w32" : "l32";
        URL qExecutable = QController.class.getClassLoader().getResource("q/" + os + "/q" + (("w32".equals(os)) ? ".exe" : ""));
        URL qHome = QController.class.getClassLoader().getResource("q");

        this.process = KdbInitUtil.getKdbProcess(os, qExecutable, qHome);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        init(os, qHome);
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

    private void init(String os, URL qHome) throws IOException, c.KException, URISyntaxException {
        c c = new c("localhost", 1234);
        KdbInitUtil.loadKdbProcess(c, os, qHome);
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
