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
package com.dremio.extras.plugins.kdb.rels.functions;

import java.util.List;

import com.dremio.extras.plugins.kdb.rels.translate.KdbColumn;

/**
 * no arg function mapping interface to kdb
 */
public class NoArgKdbFunction implements KdbFunction {
    private final String stdOperator;

    public NoArgKdbFunction(String operator) {
        this.stdOperator = operator;
    }

    @Override
    public KdbColumn render(List<KdbColumn> operands) {
        return KdbColumn.name(".z.d", ".z.d");
    }
}
