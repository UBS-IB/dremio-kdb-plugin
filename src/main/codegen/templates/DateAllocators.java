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
<@pp.dropOutputFile />



<#list cast.types as type>
<#if type.major == "Date">

<#if type.rename == true>
<@pp.changeOutputFile name="/com/dremio/extras/plugins/kdb/exec/gallocators/${type.renameVal}Allocator.java" />
<#else>
<@pp.changeOutputFile name="/com/dremio/extras/plugins/kdb/exec/gallocators/${type.from}Allocator.java" />
</#if>

<#include "/@includes/license.ftl" />

package com.dremio.extras.plugins.kdb.exec.gallocators;

import ${type.import};

import com.dremio.extras.plugins.kdb.exec.allocators.Allocator;

import io.netty.buffer.ArrowBuf;

/**
 * generated from ${.template_name} ${type.from} ${type.offset} ${type.major}
 */
@SuppressWarnings("unused")
public class <#if type.rename == true>${type.renameVal}<#else>${type.from}</#if>Allocator implements Allocator {

  @Override
  public void get(Object o, int size, int count, ArrowBuf buf, ArrowBuf offsets) {
    if (o instanceof ${type.from}[]) {
      ${type.from}[] data = (${type.from}[]) o;
      for (${type.from} l : data) {
        buf.write${type.to}(<#if type.cast == true>(${type.castVal})</#if>l.getTime());
      }
    } else if (o instanceof Object[]) {
      offsets.writeInt(buf.writerIndex());
      for (Object oo : (Object[]) o) {
        get(oo, size, count, buf, offsets);
        offsets.writeInt(buf.writerIndex() / offset());
      }
    }
  }

  @Override
  public int offset() {
    return ${type.offset};
  }

}

</#if> <#-- type.major -->
</#list>

