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
package com.dremio.extras.plugins.kdb.rels.translate;

import java.util.List;
import java.util.Map;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * bean to hold info about a kdb column while translating to q
 */
public class KdbColumn {

    private final String name;
    private final List<KdbColumn> parents = Lists.newArrayList();
    private final RelDataTypeField field;
    private String expression;

    public KdbColumn(String name, String expression, List<KdbColumn> parents, RelDataTypeField field) {
        this.name = name;
        this.expression = (expression == null) ? name : expression;
        if (parents != null) {
            this.parents.addAll(parents);
        }
        this.field = field;
    }

    public static KdbColumn name(String name, String expr, RelDataTypeField field) {
        return new KdbColumn(name, expr, null, field);
    }

    public static Map<String, String> toMap(List<KdbColumn> aggregates) {
        Map<String, String> map = Maps.newHashMap();
        for (KdbColumn c : aggregates) {
            map.put(c.name, c.expression);
        }
        return map;
    }

    public static KdbColumn name(String name, String expr) {
        return new KdbColumn(name, expr, null, null);
    }

    public static KdbColumn name(String name, String expr, String... parents) {
        List<KdbColumn> cols = Lists.newArrayList();
        for (String s : parents) {
            cols.add(name(s, s));
        }
        return new KdbColumn(name, expr, cols, null);
    }


    public static List<String> names(List<KdbColumn> aggCols) {
        List<String> names = Lists.newArrayList();
        for (KdbColumn c : aggCols) {
            names.add(c.name);
        }
        return names;
    }

    public static List<Pair<String, String>> toPairList(List<KdbColumn> aggCols) {
        List<Pair<String, String>> names = Lists.newArrayList();
        for (KdbColumn c : aggCols) {
            names.add(Pair.of(c.name, c.expression));
        }
        return names;
    }

    public String getName() {
        return name;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public List<KdbColumn> getParents() {
        return parents;
    }

    public RelDataTypeField getField() {
        return field;
    }
}
