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

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Map of all Kdb errors to sensible strings
 */
public final class Errors {
    public static final Map<String, String> ERROR_MAP = Maps.newHashMap();

    static {
        ERROR_MAP.put("access", "Attempt to read files above directory, run system commands or failed usr/pwd");
        ERROR_MAP.put("accp", "Tried to accept an incoming TCP/IP connection but failed to do so");
        ERROR_MAP.put("adict", "Blocked assignment (`'nyi`)");
        ERROR_MAP.put("arch", "Attempt to load file of wrong endian format");
        ERROR_MAP.put("assign", "Attempt to redefine a reserved word");
        ERROR_MAP.put("badtail", "Incomplete transaction at end of file, get good (count;length) with ``-11!(-2;`:file)``");
        ERROR_MAP.put("can't", "Only commercially licensed kdb+ instances can encrypt code in a script");
        ERROR_MAP.put("cast", "Value not in enumeration");
        ERROR_MAP.put("con", "qcon client is not supported when kdb+ is in [multithreaded input mode](/cookbook/multithreaded-input/)");
        ERROR_MAP.put("cond", "Even number of arguments to `$`");
        ERROR_MAP.put("conn", "Too many connections (1022 max)");
        ERROR_MAP.put("Could not initialize ssl", "[`(-26!)[]`](internal/#-26x-ssl) found SSL/TLS not enable");
        ERROR_MAP.put("d8", "The log had a partial transaction at the end but q couldn’t truncate the fil");
        ERROR_MAP.put("domain", "Out of domain");
        ERROR_MAP.put("elim", "Too many enumerations (max: 57)");
        ERROR_MAP.put("enable slave threads via cmd line -s only", "Command line enabled processes for parallel processing");
        ERROR_MAP.put("failed to load TLS certificates", "Started kdb+ [with `-E 1` or `-E 2`](cmdline/#-e-tls-server-mode) but without SSL/TLS enabled");
        ERROR_MAP.put("from", "badly formed select statement");
        ERROR_MAP.put("glim", "`` `g#`` limit (99 prior to V3.2, now unlimited");
        ERROR_MAP.put("hop", "Request to `hopen` a handle fails; includes message from OS");
        ERROR_MAP.put("hwr", "Handle write error, can't write inside a [`peach`](peach)");
        ERROR_MAP.put("insert", "Attempt to insert a record with an existing key into a keyed table");
        ERROR_MAP.put("length", "Incompatible lengths");
        ERROR_MAP.put("limit", "Tried to generate a list longer than 2,000,000,000, or serialized object is &gt; 1TB (2GB until V3.4), or `'type` if trying to serialize a nested object which has &gt; 2 billion element");
        ERROR_MAP.put("loop", "Dependency loop");
        ERROR_MAP.put("mismatch", "Columns that can't be aligned for R,R or K,K");
        ERROR_MAP.put("Mlim", "Too many nested columns in [splayed tables](/cookbook/splayed-tables). (Prior to V3.0, limited to 999; from V3.0, 251; from V3.3, 65530)");
        ERROR_MAP.put("mq", "Multi-threading not allowed");
        ERROR_MAP.put("name too long", "Filepath ≥100 chars (until V3.6 2018.09.26)");
        ERROR_MAP.put("noamend", "Cannot change global state from within an amend");
        ERROR_MAP.put("no append to zipped enums", "Cannot append to zipped enum (from V3.0)");
        ERROR_MAP.put("noupdate", "Updates blocked by the `-b` cmd line arg, or reval code or a thread other than the main thread has attempted to update a global variable when in [`peach`](peach)+slave threads or multithreaded input queue");
        ERROR_MAP.put("nosocket", "Can only open or use sockets in main thread.");
        ERROR_MAP.put("nyi", "Not yet implemented: it probably makes sense, but it’s not defined nor implemented, and needs more thinking about as the language evolves");
        ERROR_MAP.put("os", "Operating-system error or [license error](#license-errors)");
        ERROR_MAP.put("par", "Unsupported operation on a partitioned table or component thereof");
        ERROR_MAP.put("parse", "Invalid [syntax](syntax)");
        ERROR_MAP.put("part", "Something wrong with the partitions in the HDB");
        ERROR_MAP.put("path too long", "File path >=255 chars (100 until V3.6 2018.09.26)");
        ERROR_MAP.put("pl", "[`peach`](peach) can’t handle parallel lambdas (V2.3 only)");
        ERROR_MAP.put("pwuid", "OS is missing libraries for `getpwuid`. (Most likely 32-bit app on 64-bit OS. Try to [install ia32-libs](/tutorials/install/#32-bit-or-64-bit).)");
        ERROR_MAP.put("Q7", "nyi op on file nested arra");
        ERROR_MAP.put("rank", "Invalid [rank](syntax/#rank)");
        ERROR_MAP.put("rb", "Encountered a problem while doing a blocking read");
        ERROR_MAP.put("restricted", "in a kdb+ process which was started with [`-b` cmd line](cmdline/#-b-blocked). Also for a kdb+ process using the username:password authentication file, or the `-b` cmd line option, `\\x` cannot be used to reset handlers to their default. e.g. `\\x .z.pg`");
        ERROR_MAP.put("s-fail", "Invalid attempt to set \"sorted\" [attribute](elements/#attributes). Also encountered with `` `s#enums`` when loading a database (`\\l db`) and enum target is not already loaded.");
        ERROR_MAP.put("splay", "nyi op on [splayed table](/cookbook/splayed-tables)");
        ERROR_MAP.put("stack", "Ran out of stack space");
        ERROR_MAP.put("step", "Attempt to upsert a step dictionary in place");
        ERROR_MAP.put("stop", "User interrupt (Ctrl-c) or [time limit (`-T`)](cmdline/#-t-timeout)");
        ERROR_MAP.put("stype", "Invalid [type](datatypes) used to [signal](errors/#signal)");
        ERROR_MAP.put("sys", "Using system call from thread other than main thread");
        ERROR_MAP.put("threadview", "Trying to calc a [view](/tutorials/views) in a thread other than main thread. A view can be calculated in the main thread only. The cached result can be used from other threads.");
        ERROR_MAP.put("timeout", "request to `hopen` a handle fails on a timeout; includes message from OS");
        ERROR_MAP.put("TLS not enabled", "Received a TLS connection request, but kdb+ not [started with `-E 1` or `-E 2`](cmdline/#-e-tls-server-mode");
        ERROR_MAP.put("too many syms", "Kdb+ currently allows for ~1.4B interned symbols in the pool and will exit with this error when this threshold is reached");
        ERROR_MAP.put("trunc", "The log had a partial transaction at the end but q couldn’t truncate the file");
        ERROR_MAP.put("type", "Wrong [type](datatypes). Also see `'limit`");
        ERROR_MAP.put("type/attr error amending file", "Direct update on disk for this type or attribute is not allowed");
        ERROR_MAP.put("u-fail", "Invalid attempt to set “unique” [attribute](elements/#attributes)");
        ERROR_MAP.put("unmappable", "When saving partitioned data each column must be mappable. `()` and `(\"\";\"\";\"\")` are OK");
        ERROR_MAP.put("upd", "Function `upd` is undefined (sometimes encountered during ``-11!`:logfile``) _or_ [license error](#license-errors)");
        ERROR_MAP.put("utf8", "The websocket requires that text is UTF-8 encoded");
        ERROR_MAP.put("value", "No value");
        ERROR_MAP.put("vd1", "Attempted multithread update");
        ERROR_MAP.put("view", "Trying to re-assign a [view](/tutorials/views) to something else");
        ERROR_MAP.put("wsfull", "[`malloc`](https://en.wikipedia.org/wiki/C_dynamic_memory_allocation) failed, ran out of swap (or addressability on 32-bit), or hit [`-w` limit](cmdline/#-w-memory");
        ERROR_MAP.put("wsm", "Alias for nyi for `wsum` prior to V3.2");
        ERROR_MAP.put("XXX", "Value error (`XXX` undefined)");
        ERROR_MAP.put("branch", "A branch (if;do;while;$[.;.;.]) more than 65025 byte codes away (255 before V3.5 2017.09.26)");
        ERROR_MAP.put("char", "Invalid character");
        ERROR_MAP.put("constants", "Too many constants (max locals+globals+constants: 240; 95 before V3.5 2017.09.26)");
        ERROR_MAP.put("globals", "Too many global variables (110 max; 31 before V3.5 2017.09.26)");
        ERROR_MAP.put("locals", "Too many local variables (110 max; 23 before V3.5 2017.09.26)");
        ERROR_MAP.put("params", "Too many parameters (8 max)");
        ERROR_MAP.put("`XXX:YYY`", "`XXX` is from kdb+, `YYY` from the OS");
        ERROR_MAP.put("cores", "The license is for fewer cores than available");
        ERROR_MAP.put("cpu", "The license is for fewer CPUs than available");
        ERROR_MAP.put("exp", "License expiry date is prior to system date");
        ERROR_MAP.put("host", "The hostname reported by the OS does not match the hostname or hostname-pattern in the license.");
        ERROR_MAP.put("k4.lic", "k4.lic file not found, check contents of environment variables");
        ERROR_MAP.put("os", "Wrong OS or operating-system error (if runtime error)");
        ERROR_MAP.put("srv", "Client-only license in server mode");
        ERROR_MAP.put("upd", "Version of kdb+ more recent than update date, _or_ the function `upd` is undefined (sometimes encountered during ``-11!`:logfile``)");
        ERROR_MAP.put("user", "Unlicenced user");
        ERROR_MAP.put("wha", "System date is prior to kdb+ version date");
    }

    private Errors() {

    }
}
