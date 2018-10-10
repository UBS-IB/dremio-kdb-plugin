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

import java.util.List;

import javax.inject.Provider;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

import org.hibernate.validator.constraints.NotEmpty;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.AuthenticationType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;

import io.protostuff.Tag;

/**
 * Plugin Configuration for KDB
 */
@SourceType(value = "KDB", label = "Kdb")
public class KdbStoragePluginConfig extends ConnectionConf<KdbStoragePluginConfig, KdbStoragePlugin> {

    @NotEmpty
    @Tag(1)
    @DisplayMetadata(label = "Kdb host")
    public String host;

    @Min(1)
    @Max(65535)
    @Tag(2)
    @DisplayMetadata(label = "Port")
    public int port = 1234;

    @Tag(3)
    public String username;

    @Tag(4)
    @Secret
    public String password;

    @Tag(5)
    public AuthenticationType authenticationType;

    @Tag(6)
    @DisplayMetadata(label = "fetch size")
    @Max(1000000000)
    @Min(1000)
    public int fetchSize = 1000;

    @Tag(7)
    public List<Property> propList;

    @Override
    public KdbStoragePlugin newPlugin(
            SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {

        return new KdbStoragePlugin(this, context, name);
    }

}
