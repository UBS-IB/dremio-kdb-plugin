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
package com.dremio.extras.plugins.kdb.exec.allocators;

import io.netty.buffer.ArrowBuf;

/**
 * helper interface to allocate from kdb->arrow
 */
public class BooleanAllocator implements Allocator {
    @Override
    public void get(Object o, int size, int count, ArrowBuf buf, ArrowBuf offsets) {
        if (o instanceof boolean[]) {
            boolean[] data = (boolean[]) o;
            byte value = 0;
            int i = 0;

            for (boolean l : data) {
                value += l ? (1 << i) : 0;
                i++;
                if (i == 8) {
                    buf.writeByte(value);
                    value = 0;
                    i = 0;
                }
            }
            if (i > 0) {
                buf.writeByte(value);
            }
        }
    }

    @Override
    public int offset() {
        return -1;
    }
}
