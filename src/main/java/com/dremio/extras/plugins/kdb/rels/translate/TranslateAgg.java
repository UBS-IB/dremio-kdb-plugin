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
package com.dremio.extras.plugins.kdb.rels.translate;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.extras.plugins.kdb.KdbTableDefinition;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * translate aggs
 */
public class TranslateAgg implements Translate {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOGGER = LoggerFactory.getLogger(KdbAggregate.class);
    private final double version;
    private final List<KdbAggregate> aggs = Lists.newArrayList();
    private List<KdbColumn> finalProjectedColumns = Lists.newArrayList();
    private List<KdbColumn> finalAggregateColumns;

    public TranslateAgg(
            List<KdbPrel> stack, ReadDefinition readDefinition) {
        String versionStr = readDefinition.getExtendedProperty().toStringUtf8();
        double versionTmp = -1;
        try {
            KdbTableDefinition.KdbXattr xattr = MAPPER.reader(KdbTableDefinition.KdbXattr.class).readValue(versionStr);
            versionTmp = xattr.getVersion();
        } catch (IOException e) {
            LOGGER.error("couldn't parse xattr", e);
        }
        version = versionTmp;

        for (KdbPrel prel : stack) {
            if (prel instanceof KdbAggregate) {
                aggs.add((KdbAggregate) prel);
            }
        }
    }

    static List<String> kdbFieldNames(final RelDataType rowType) {
        return SqlValidatorUtil.uniquify(new AbstractList<String>() {
            @Override
            public String get(int index) {
                String name = rowType.getFieldList().get(index).getName();
                name = name.replace("DUMMY", "i");
                return name.replaceAll("\\$", "xx_xx");
            }

            @Override
            public int size() {
                return rowType.getFieldCount();
            }
        }, SqlValidatorUtil.EXPR_SUGGESTER, true);
    }

    @Override
    public String go() {
        if (aggs.isEmpty()) {
            //functionalBuffer.append("0b");
            return null;
        }
        if (aggs.size() == 1) {
            KdbAggregate project = aggs.get(0);
            return implement(project);
        }
        //todo merge aggs??
        throw new UnsupportedOperationException();
    }

    private List<String> appendGroupby(StringBuffer buffer, String aggProj) {
        buffer.append(aggProj);
        String aggColStr = aggProj.split("!")[0];
        String[] aggCols = aggColStr.replace("enlist ", "").replace("`", " ").split(" ");
        List<String> cols = Lists.newArrayList();
        for (String s : aggCols) {
            String tmp = s.replace(" ", "").replace("(", "").replace(")", "");
            if (!tmp.isEmpty() && !"0b".equals(tmp)) {
                cols.add(tmp);
            }
        }
        return cols;

    }

    public String implement(KdbAggregate input) {

        List<KdbColumn> colList = new ArrayList<>();


        final List<String> inNames = kdbFieldNames(input.getInput().getRowType());
        final List<String> outNames = kdbFieldNames(input.getRowType());
        int i = 0;
        List<String> keys = Lists.newArrayList();
        List<KdbColumn> keyCols = Lists.newArrayList();
        if (input.getGroupSet().cardinality() == 1) {
            final String inName = inNames.get(input.getGroupSet().nth(0));
            keys.add(inName);
            keyCols.add(new KdbColumn(inName, null, null, input.getRowType().getField(inName, true, true)));
            ++i;
        } else {
            for (int group : input.getGroupSet()) {
                final String inName = inNames.get(group);
                keys.add(inName);
                keyCols.add(new KdbColumn(inName, null, null, input.getRowType().getField(inName, true, true)));
                ++i;
            }
        }
        for (AggregateCall aggCall : input.getAggCallList()) {
            String k;
            if (keys.size() == 0) {
                k = input.getInput().getRowType().getFieldList().get(0).getName().replace("DUMMY", "i");
            } else {
                k = keys.get(0);
            }
            String outName = outNames.get(i++).replace("$", "xx_xx");
            String inName = toKdb(aggCall.getAggregation(), inNames, aggCall.getArgList(), k, aggCall.isDistinct());
            colList.add(
                    new KdbColumn(
                            outName,
                            inName,
                            aggParents(aggCall, input, keyCols),
                            input.getRowType().getField(outName, true, true)));
        }
        String by = keys.isEmpty() ? "" : "`" + Joiner.on("`").join(keys);
        if (keys.size() == 1) {
            by = "enlist " + by;
        }
//    String aggProj;
//    //todo this doesn't work with complex aggregates
//    if (inList.size() == 1) {
//      aggProj = "(enlist `" + outList.get(0) + ")!(enlist " + inList.get(0) + ")";
//    } else {
//      aggProj = "(`" + Joiner.on("`").join(outList) + ")!(" + Joiner.on(";").join(inList) + ")";
//    }
        if (keys.isEmpty() && colList.isEmpty()) {
            return null;
        }
        finalAggregateColumns = keyCols;
        finalProjectedColumns = colList;
//        functionalBuffer.append((keys.isEmpty()) ? "0b" : ("(" + by + ")!(" + by + ")"));
        return by;
    }

    private List<KdbColumn> aggParents(AggregateCall aggCall, KdbAggregate input, List<KdbColumn> keyCols) {
        List<KdbColumn> columns = Lists.newArrayList();
        for (Integer i : aggCall.getArgList()) {
            RelDataTypeField field = input.getInput().getRowType().getFieldList().get(i);
            columns.add(new KdbColumn(field.getName().replace("$", "xx_xx"), null, null, field));
        }
        columns.addAll(keyCols);
        return columns;
    }

    private String toKdb(SqlAggFunction aggregation, List<String> inNames, List<Integer> args, String aggField, boolean distinct) {
        if (aggregation == SqlStdOperatorTable.COUNT) {
            String agg = distinct ? "count; (distinct " : "count ";
            String castPrefix = (version < 3) ? "($;\"j\";" : "";
            String castSuffix = (version < 3) ? ")" : "";
            if (args.size() == 0) {
                return castPrefix + "(" + agg + ";`" + aggField + ")" + ((distinct) ? ")" : "") + castSuffix;
            } else {
                assert args.size() == 1;
                final String inName = inNames.get(args.get(0));
                return castPrefix + "(" + agg + ";`" + inName + ")" + ((distinct) ? ")" : "") + castSuffix;
            }
        } else if (aggregation instanceof SqlSumAggFunction || aggregation instanceof SqlSumEmptyIsZeroAggFunction) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(sum;`" + inName + ")";
        } else if (aggregation == SqlStdOperatorTable.MIN) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(min;`" + inName + ")";
        } else if (aggregation == SqlStdOperatorTable.MAX) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(max;`" + inName + ")";
        } else if (aggregation == SqlStdOperatorTable.AVG) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(avg;`" + inName + ")";
        } else if (aggregation == SqlStdOperatorTable.STDDEV_POP) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(sdev;`" + inName + ")";
        } else if (aggregation == SqlStdOperatorTable.STDDEV) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(dev;`" + inName + ")";
        } else if (aggregation == SqlStdOperatorTable.VAR_POP) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(svar;`" + inName + ")";
        } else if (aggregation == SqlStdOperatorTable.VARIANCE) {
            assert args.size() == 1;
            final String inName = inNames.get(args.get(0));
            return "(var;`" + inName + ")";
        } else if ("CORR".equals(aggregation.getName())) {
            assert args.size() == 2;
            final String inName = inNames.get(args.get(0));
            final String inName2 = inNames.get(args.get(1));
            return "(cor;`" + inName + ";`" + inName2 + ")";
        } else {
            throw new AssertionError("unknown aggregate " + aggregation);
        }
    }

    public List<KdbColumn> getProjectedColumns() {
        return finalProjectedColumns;
    }

    public List<KdbColumn> getAggregateColumns() {
        return finalAggregateColumns;
    }
}
