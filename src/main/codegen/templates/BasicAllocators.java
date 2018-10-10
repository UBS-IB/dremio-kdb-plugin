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
<@pp.dropOutputFile />



<#list cast.types as type>
<#if type.major == "Fixed">

<@pp.changeOutputFile name="/com/dremio/extras/plugins/kdb/exec/gallocators/${type.from}Allocator.java" />

<#include "/@includes/license.ftl" />

package com.dremio.extras.plugins.kdb.exec.gallocators;

import com.dremio.extras.plugins.kdb.exec.allocators.Allocator;

import io.netty.buffer.ArrowBuf;

/**
 * generated from ${.template_name} ${type.from} ${type.offset} ${type.major}
 */
@SuppressWarnings("unused")
public class ${type.from}Allocator implements Allocator {

  @Override
  public void get(Object o, int size, int count, ArrowBuf buf, ArrowBuf offsets) {
    if (o instanceof ${type.native}[]) {
      ${type.native}[] data = (${type.native}[]) o;
      for (${type.native} l : data) {
        buf.write${type.from}(l);
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

