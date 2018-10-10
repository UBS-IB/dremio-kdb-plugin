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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import com.dremio.extras.plugins.kdb.rels.translate.KdbColumn;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * xbar function mapping interface to kdb
 */
public class DateAddFunction implements KdbFunction {
    private static Map<String, String> DATE_TYPES = ImmutableMap.<String, String>builder()
            .put("hour", "minute")
            .build();
    private final boolean add;

    public DateAddFunction(boolean add) {
        this.add = add;
    }

    @Override
    public KdbColumn render(List<KdbColumn> operands) {
        List<String> exprs = operands.stream().map(KdbColumn::getExpression).collect(Collectors.toList());
        List<String> names = operands.stream().map(KdbColumn::getName).collect(Collectors.toList());
        Set<RelDataTypeField> fields = operands.stream().map(KdbColumn::getField).filter(Objects::nonNull).collect(Collectors.toSet());

        List<KdbColumn> parents = Lists.newArrayList();
        for (String s : names) {
            parents.add(KdbColumn.name(s, s));
        }
        RelDataTypeField field = (fields.isEmpty()) ? null : fields.iterator().next();

        if (field != null && SqlTypeName.DATE.equals(field.getType().getSqlTypeName())) {
            String op = add ? "+" : "-";
            return new KdbColumn(null, "(" + op + ";" + exprs.get(0) + ";" + exprs.get(1) + ")", parents, field);
        }
        throw new UnsupportedOperationException("cant convert date type " + ((field == null) ? "null" : field.getType().getSqlTypeName().getName()));


    }
}
