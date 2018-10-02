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

import javax.inject.Provider;

import org.hibernate.validator.constraints.NotEmpty;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.protostuff.Tag;

/**
 * Plugin Configuration for KDB
 */
@SourceType(value = "KDB", label = "Kdb")
public class KdbStoragePluginConfig extends ConnectionConf<KdbStoragePluginConfig, KdbStoragePlugin> {

    @Tag(3)
    public String username;
    @Tag(4)
    @Secret
    public String password;
    @NotEmpty
    @Tag(1)
    public String hostname;
    @NotEmpty
    @Tag(2)
    public String port;
    @Tag(5)
    public String authenticationType;
    @Tag(6)
    public String fetchSize;

    public KdbStoragePluginConfig() {
    }

    public KdbStoragePluginConfig(@JsonProperty("username") String username, @JsonProperty("password") String password, @JsonProperty("hostname") String hostname, @JsonProperty("port") String port, @JsonProperty("fetchSize") String fetchSize) {

        this.username = username;
        this.password = password;
        this.hostname = hostname;
        this.port = port;
        this.fetchSize = fetchSize;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getHostname() {
        return hostname;
    }

    public String getPort() {
        return port;
    }

    public String getFetchSize() {
        return fetchSize;
    }

    @Override
    public KdbStoragePlugin newPlugin(
            SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {

        return new KdbStoragePlugin(this, context, name);

    }
}
