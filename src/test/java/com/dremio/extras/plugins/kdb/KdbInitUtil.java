package com.dremio.extras.plugins.kdb;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

public class KdbInitUtil {
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

    public static void loadKdbProcess(c c) throws c.KException, IOException {
        c.k("\\l trade.q");
        c.k("\\l sp.q");
        c.k("md:`:fxkdbldns4.ldn.swissbank.com:19051:kdbprod:kdbprod \"select[1000] from CURRENEX_stm\"");
        c.k("daptrade:`:xldn8589pap.ldn.swissbank.com:16551:daplive:cherioteri \"select[100] from trades\"");
    }


}
