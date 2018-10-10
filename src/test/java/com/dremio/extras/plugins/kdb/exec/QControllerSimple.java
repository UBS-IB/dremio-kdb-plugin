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
import java.net.URISyntaxException;
import java.net.URL;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.dremio.extras.plugins.kdb.KdbInitUtil;
import com.dremio.extras.plugins.kdb.c;


/**
 * build q executable and load w/ initial data
 */
public class QControllerSimple {

    private static QControllerSimple self;
    private Process process;

    @BeforeClass
    public static void setupKdbTestCluster() throws Exception {
        self = new QControllerSimple();
        self.start();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        self.stop();
    }

    public void start() throws IOException, com.dremio.extras.plugins.kdb.c.KException, URISyntaxException {
        String os = (System.getProperty("os.name").contains("indow")) ? "w32" : "l32";
        URL qExecutable = QControllerSimple.class.getClassLoader().getResource("q/" + os + "/q" + (("w32".equals(os)) ? ".exe" : ""));
        URL qHome = QControllerSimple.class.getClassLoader().getResource("q");

        this.process = KdbInitUtil.getKdbProcess(os, qExecutable, qHome);
        init(os, qHome);
    }


    private void init(String os, URL qHome) throws IOException, com.dremio.extras.plugins.kdb.c.KException, URISyntaxException {
        com.dremio.extras.plugins.kdb.c c = new c("localhost", 1234);
        KdbInitUtil.loadKdbProcess(c, os, qHome);
    }


    public void stop() {
        process.destroy();
    }

}
