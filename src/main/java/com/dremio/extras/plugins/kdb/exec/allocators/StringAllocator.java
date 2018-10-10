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

import java.io.IOException;

import io.netty.buffer.ArrowBuf;

/**
 * helper interface to allocate from kdb->arrow
 */
public class StringAllocator implements Allocator {
    @Override
    public void get(Object o, int size, int count, ArrowBuf buf, ArrowBuf x) throws IOException {
        if (o instanceof String[]) {
            int byteCount = 4 * count + 4;
            ArrowBuf offsets = buf.slice(0, byteCount).writerIndex(0);
            ArrowBuf data = buf.slice(byteCount, size).writerIndex(0);
            int offset = 0;
            offsets.writeInt(0);
            for (String s : ((String[]) o)) {
                byte[] bytes = s.getBytes("UTF-8");
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
                int byteCount = 4 * count + 4;
                ArrowBuf offsets = buf.slice(0, byteCount).writerIndex(0);
                ArrowBuf data = buf.slice(byteCount, size).writerIndex(0);
                int offset = 0;
                offsets.writeInt(0);
                for (Object s : ((Object[]) o)) {
                    char[] ss = (char[]) s;
                    byte[] bytes = new String(ss).getBytes("UTF-8");
                    int wordLength = bytes.length;
                    offsets.writeInt(wordLength + offset);
                    offset += wordLength;
                    data.writeBytes(bytes);
                }
            } else if (((Object[]) o)[0] instanceof String[]) {
                x.writeInt(buf.writerIndex());
                int byteCount = 4;
                for (Object s : ((Object[]) o)) {
                    for (Object oo : ((Object[]) s)) {
                        byteCount += 4;//* Math.max(1,((String)oo).length());
                    }
                    if (((Object[]) s).length == 0) {
                        byteCount += 4;
                    }
                }
                ArrowBuf offsets = buf.slice(0, byteCount).writerIndex(0);
                ArrowBuf data = buf.slice(byteCount, size).writerIndex(0);

                int offset = 0;
                int totalOffset = 0;
                offsets.writeInt(0);
                for (Object s : ((Object[]) o)) {
                    int totalWordLength = 0;
                    for (Object oo : ((Object[]) s)) {
                        byte[] bytes = ((String) oo).getBytes("UTF-8");
                        int wordLength = bytes.length;
                        offsets.writeInt(wordLength + offset);
                        offset += wordLength;
                        totalWordLength += wordLength;
                        data.writeBytes(bytes);
                    }
                    if (((Object[]) s).length == 0) {
                        offsets.writeInt(offset);
                    }
                    x.writeInt(totalOffset + totalWordLength);
                    totalOffset += totalWordLength;
                }

            }
        }
    }


    @Override
    public int offset() {
        return -1;
    }
}
