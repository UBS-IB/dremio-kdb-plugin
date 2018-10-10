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
import java.util.Map;

/**
 * Start and run q for testing, load datasets
 */
public final class KdbInitUtil {

    private KdbInitUtil() {
    }

    public static Process getKdbProcess(String os, URL qExecutable, URL qHome) throws IOException {
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
        return Runtime.getRuntime().exec(cmd, envStr);
    }

    public static void loadKdbProcess(c c, String os, URL qHome) throws c.KException, IOException, URISyntaxException {
        // The path to KDB files we store. Note that kdb needs linux path for loading the file. File was generated with "save `:tablename" in KDB.
        String parentQPath;
        if ("w32".equals(os)) {
            parentQPath = qHome.toURI().resolve(".").toURL().getFile().replace("/", "\\").replaceFirst("\\\\", "");
        } else {
            parentQPath = qHome.toURI().resolve(".").toURL().getFile();
        }
        String qDataFilesPath = parentQPath
                .replace("\\", "/")
                .replace("'", "");
        c.k("\\l sp.q");
        c.k("daptrades_strings: get `$\":" + qDataFilesPath + "/data/daptrades_strings.dat\"");  // We use this dataset as column "XXX" is a symbol while the column "YYY" is a varchar
        c.k("daptrade_problematic_array_columns: get `$\":" + qDataFilesPath + "/data/daptrade2.dat \"");
        c.k("\\l " + qDataFilesPath + "/data/db");
        if ("true".equals(System.getProperty("kdb.build.ubs"))) {
            c.k("daptrade: get `$\":" + qDataFilesPath + "/ubs/daptrade.dat \"");
            c.k("clientspot: get `$\":" + qDataFilesPath + "/ubs/clientspot.dat \"");
            c.k("daprfq: get `$\":" + qDataFilesPath + "/ubs/daprfq3.dat \"");
            c.k("daptrades: get `$\":" + qDataFilesPath + "/ubs/daptrade3.dat \"");
            c.k("mdd: get `$\":" + qDataFilesPath + "/ubs/md2.dat  \"");
            c.k("md: get `$\":" + qDataFilesPath + "/ubs/md.dat\"");
        }
    }
}
