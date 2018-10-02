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
package com.dremio.extras.plugins.kdb.rels.functions;

import java.util.List;
import java.util.stream.Collectors;

import com.dremio.extras.plugins.kdb.rels.translate.KdbColumn;
import com.google.common.base.Joiner;

/**
 * binary function mapping to kdb
 */
public class BinaryFunction implements KdbFunction {
    private final String stdOperator;

    public BinaryFunction(String operator) {
        this.stdOperator = operator;
    }

    @Override
    public KdbColumn render(List<KdbColumn> strings) {
        List<String> exprs = strings.stream().map(KdbColumn::getExpression).collect(Collectors.toList());
        List<String> names = strings.stream().map(KdbColumn::getName).collect(Collectors.toList());
        return KdbColumn.name(null, "(" + stdOperator + ";" + Joiner.on(";").join(exprs) + ")", names.toArray(new String[0]));
    }
}
