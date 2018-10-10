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
import java.util.stream.Collectors;

import com.dremio.extras.plugins.kdb.rels.translate.KdbColumn;
import com.google.common.base.Joiner;

/**
 * case function mapping to kdb
 */
public class CaseKdbFunction implements KdbFunction {
    @Override
    public KdbColumn render(List<KdbColumn> operands) {
        if (operands.size() != 3) {
            throw new UnsupportedOperationException("cant do a case statement in q with more than 3 operands");
        }
        List<String> exprs = operands.stream().map(KdbColumn::getExpression).collect(Collectors.toList());
        List<String> names = operands.stream().map(KdbColumn::getName).collect(Collectors.toList());
        return KdbColumn.name(null, "( ?;" + Joiner.on(";").join(exprs) + ")", names.toArray(new String[0]));
    }
}
