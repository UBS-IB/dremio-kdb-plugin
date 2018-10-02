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
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.extras.plugins.kdb.rels.translate.KdbColumn;
import com.google.common.collect.ImmutableMap;

/**
 * xbar function mapping interface to kdb
 */
public class XbarFunction implements KdbFunction {
    private static Map<String, String> DATE_TYPES = ImmutableMap.<String, String>builder()
            .put("hour", "minute")
            .build();

    @Override
    public KdbColumn render(List<KdbColumn> operands) {
        List<String> exprs = operands.stream().map(KdbColumn::getExpression).collect(Collectors.toList());
        List<String> names = operands.stream().map(KdbColumn::getName).collect(Collectors.toList());
        String trunctype = names.get(0);
        int val = 1;
        if ("hour".equals(trunctype)) {
            val = 60;
        }
        String xbarVal = getVal(trunctype, exprs);
        return KdbColumn.name(null, "( xbar; " + val + ";" + xbarVal + ")", names.toArray(new String[0]));


    }

    private String getVal(String trunctype, List<String> exprs) {
        String dateReplace = DATE_TYPES.getOrDefault(trunctype, trunctype);
        return "($;enlist `" + dateReplace + ";" + exprs.get(1) + ")";
    }
}
