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
package com.dremio.extras.plugins.kdb.exec.nullCheck;

import java.io.IOException;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.vector.complex.fn.ArrowBufInputStream;

/**
 * scan kdb buffer for nulls
 */
public class StringNullCheck implements NullCheck {
    private final UserBitShared.SerializedField offset;
    private final int size;
    private int count = 0;
    private int[] offsets = null;

    public StringNullCheck(UserBitShared.SerializedField serializedField) {
        UserBitShared.SerializedField tmpOffset;
        try {
            tmpOffset = serializedField.getChild(1).getChild(0);
        } catch (Throwable t) {
            tmpOffset = serializedField.getChild(2).getChild(1).getChild(0);
        }
        offset = tmpOffset;
        size = offset.getValueCount();
    }

    @Override
    public boolean check(ArrowBufInputStream buf) throws IOException {
        if (offsets == null) {
            offsets = new int[size];
            for (int i = 0; i < size; i++) {
                offsets[i] = buf.readInt();
            }
        }
        int wordSize = offsets[count + 1] - offsets[count++];
        byte[] word = new byte[wordSize];
        buf.read(word);
        String s = new String(word, "UTF-8");
        return !(" ".equals(s) || "".equals(s));
    }
}
