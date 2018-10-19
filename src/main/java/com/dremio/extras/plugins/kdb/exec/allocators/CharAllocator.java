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
package com.dremio.extras.plugins.kdb.exec.allocators;

import java.io.IOException;

import io.netty.buffer.ArrowBuf;

/**
 * helper interface to allocate from kdb->arrow
 */
public class CharAllocator implements Allocator {
    @Override
    public void get(Object o, int size, int count, ArrowBuf buf, ArrowBuf x) throws IOException {
        if (o instanceof char[]) {
            int byteCount = 4 * count + 4;
            ArrowBuf offsets = buf.slice(0, byteCount).writerIndex(0);
            ArrowBuf data = buf.slice(byteCount, size).writerIndex(0);
            int offset = 0;
            offsets.writeInt(0);
            for (char s : ((char[]) o)) {
                byte[] bytes = Character.toString(s).getBytes("UTF-8");
                int wordLength = bytes.length;
                offsets.writeInt(wordLength + offset);
                offset += wordLength;
                data.writeBytes(bytes);
            }
        }
        if (o instanceof Object[]) {
            if (((Object[]) o).length == 0) {
                int byteCount = 4 * count + 4;
                ArrowBuf offsets = buf.slice(0, byteCount).writerIndex(0);
                offsets.writeInt(0);
            } else if (((Object[]) o)[0] instanceof char[]) {
                Object[] v = (Object[]) o;
                int byteCount = 4 * count + 4;
                ArrowBuf offsets = buf.slice(0, byteCount).writerIndex(0);
                ArrowBuf data = buf.slice(byteCount, size).writerIndex(0);
                int offset = 0;
                offsets.writeInt(0);
                x.writeInt(0);
                for (Object s : v) {
                    byte[] bytes = new String((char[]) s).getBytes("UTF-8");
                    int wordLength = bytes.length;
                    offsets.writeInt(wordLength + offset);
                    x.writeInt(wordLength + offset);
                    offset += wordLength;
                    data.writeBytes(bytes);
                }
            }
        }
    }


    @Override
    public int offset() {
        return -1;
    }
}
