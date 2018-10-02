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
import java.util.Set;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.KdbSort;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * translate sort
 */
public class TranslateSort implements Translate {
    private final List<String> projectedColumns;
    private final List<KdbSort> sorts = Lists.newArrayList();
    private StringBuffer functionalBuffer;

    public TranslateSort(
            StringBuffer functionalBuffer, List<KdbPrel> stack, List<String> projectedColumns) {

        this.functionalBuffer = functionalBuffer;
        this.projectedColumns = projectedColumns;
        for (KdbPrel prel : stack) {
            if (prel instanceof KdbSort) {
                sorts.add((KdbSort) prel);
            }
        }
    }

    private static String[] splitAndStrip(String str, String split) {
        String[] args = str.split(split);
        List<String> list = Lists.newArrayList();
        for (String i : args) {
            if (i.isEmpty()) {
                continue;
            }
            list.add(i);
        }
        return list.toArray(new String[0]);
    }

    private StringBuffer addSort(String sortStr, List<String> projectedColumns) {
        StringBuffer newBuffer = new StringBuffer();
        String[] sortArgs = splitAndStrip(sortStr, " ");
        Set<String> sortSet = Sets.newHashSet(splitAndStrip(sortArgs[0], "`"));
        List<String> finalSet = Lists.newArrayList();
        for (String s : projectedColumns) {
            if (sortSet.contains(s)) {
                finalSet.add(s);
            }
        }
        String finalSort = " `" + Joiner.on("`").join(finalSet) + " " + sortArgs[1];
        newBuffer.append(finalSort);
        newBuffer.append(" ");
        newBuffer.append(functionalBuffer);
        return newBuffer;
    }

    private String sort(Multimap<String, String> fields) {
        StringBuffer buffer = new StringBuffer();
        for (String d : fields.keySet()) {
            String f = '`' + Joiner.on('`').join(fields.get(d));
            buffer.append(f);
            buffer.append(" ");
            buffer.append(d);
            buffer.append(" ");
        }
        return buffer.toString();
    }

    private String direction(RelFieldCollation fieldCollation) {
        switch (fieldCollation.getDirection()) {
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return "xdesc";
            case ASCENDING:
            case STRICTLY_ASCENDING:
            default:
                return "xasc";
        }
    }

    public String implement(KdbSort sort) {
        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            final Multimap<String, String> keys = HashMultimap.create();
            final List<RelDataTypeField> fields = sort.getRowType().getFieldList();
            for (RelFieldCollation fieldCollation : sort.getCollation().getFieldCollations()) {
                final String name =
                        fields.get(fieldCollation.getFieldIndex()).getName();
                keys.put(direction(fieldCollation), name.replace("$", "xx_xx"));
                if (false) {
                    // TODO NULLS FIRST and NULLS LAST
                    switch (fieldCollation.nullDirection) {
                        case FIRST:
                            break;
                        case LAST:
                            break;
                        default:
                            //pass
                    }
                }
            }
            return sort(keys);
        }
        return "";
    }

    @Override
    public String go() {
        if (sorts.isEmpty()) {
            return "";
        }
        if (sorts.size() == 1) {
            KdbSort sort = sorts.get(0);
            String sortStr = implement(sort);
            functionalBuffer = addSort(sortStr, projectedColumns);
            return functionalBuffer.toString();
        }
        //todo merge sorts??
        throw new UnsupportedOperationException();
    }

    @Override
    public StringBuffer buffer() {
        return functionalBuffer;
    }
}
