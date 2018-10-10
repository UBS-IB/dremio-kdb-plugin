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
package com.dremio.extras.plugins.kdb.exec.nullCheck;

import java.io.IOException;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.vector.complex.fn.ArrowBufInputStream;

import io.netty.buffer.ArrowBuf;

/**
 * scan kdb buffer for nulls
 */
public class ArrayNullCheck implements NullCheck {
    private final UserBitShared.SerializedField offset;
    private final int size;
    private NullCheck inner;
    private OffsetBuff offsets;
    private int last = -1;

    public ArrayNullCheck(UserBitShared.SerializedField serializedField, NullCheck inner) {
        offset = serializedField.getChild(0);
        this.inner = inner;
        size = offset.getValueCount();
    }

    public NullCheck getInner() {
        return inner;
    }

    @Override
    public boolean check(ArrowBufInputStream buf) throws IOException {
        if (last == -1) {
            last = offsets.read();
        }
        int newLast = offsets.read();
        int wordSize = newLast - last;
        last = newLast;
        byte[] word = new byte[wordSize];
        buf.read(word);
        return wordSize > 0;
    }

    public void setOffset(OffsetBuff buf) {
        offsets = buf;
    }

    /**
     * maintain offset
     */
    public static class OffsetBuff {
        private final ArrowBuf buf;
        private final int offset;

        public OffsetBuff(ArrowBuf buf, int offset) {
            this.buf = buf;
            this.offset = Math.abs(offset);
        }

        public int read() {
            return buf.readInt() * offset;
        }
    }
}
