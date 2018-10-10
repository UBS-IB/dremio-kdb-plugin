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

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.extras.plugins.kdb.KdbTableDefinition;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.RexToKdbColTranslator;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * translate filter
 */
public class TranslateWhere implements Translate {
    private final StringBuffer functionalBuffer;
    private final ReadDefinition readDefinition;
    private final KdbTableDefinition.KdbXattr xattr;
    private final List<KdbFilter> filters = Lists.newArrayList();

    public TranslateWhere(StringBuffer functionalBuffer, List<KdbPrel> stack, ReadDefinition readDefinition, KdbTableDefinition.KdbXattr xattr) {

        this.functionalBuffer = functionalBuffer;
        this.readDefinition = readDefinition;
        this.xattr = xattr;
        for (KdbPrel prel : stack) {
            if (prel instanceof KdbFilter) {
                filters.add((KdbFilter) prel);
            }
        }
    }

    private String translate() {
        if (filters.isEmpty()) {
            return "()";
        }
        if (filters.size() == 1) {
            KdbFilter filter = filters.get(0);
            String translated = new Translator(TranslateProject.kdbFieldNames(filter.getRowType()), readDefinition, xattr.getSymbolList()).translateMatch(filter.getCondition());
            return translated;
        }
        //todo merge filters??
        throw new UnsupportedOperationException();
    }

    @Override
    public String go() {
        functionalBuffer.append(translate());
        return functionalBuffer.toString();
    }

    /**
     * Translates {@link RexNode} expressions into kdb expression strings.
     */
    public static class Translator {

        private final Multimap<String, Pair<String, RexLiteral>> multimap =
                HashMultimap.create();
        private final Map<String, RexLiteral> eqMap =
                new LinkedHashMap<String, RexLiteral>();
        private final List<String> fieldNames;
        private ReadDefinition readDefinition;
        private final List<String> symbolCols;

        Translator(List<String> fieldNames, ReadDefinition readDefinition, List<String> symbolCols) {
            this.fieldNames = fieldNames;
            this.readDefinition = readDefinition;
            this.symbolCols = symbolCols;
        }

        public static Object literalValue(RexLiteral literal, String... operator) {
            if (literal.getTypeName() == SqlTypeName.CHAR) {
                String op = operator.length == 0 ? "" : operator[0];

                if ("like".equals(op)) {
                    return "enlist \"" + ((String) literal.getValue2())
                            .replaceAll("'", "")
                            .replace('%', '*')
//                            .replace("_","?")
                            + "\"";
                } else {
                    return "enlist `" + ((String) literal.getValue2()).replaceAll("'", "");
                }
            } else if (literal.getTypeName() == SqlTypeName.VARCHAR) {
                String op = operator.length == 0 ? "" : operator[0];

                if ("like".equals(op)) {
                    return "enlist \"" + ((String) literal.getValue2())
                            .replaceAll("'", "")
                            .replace('%', '*')
//                            .replace("_","?")
                            + "\"";
                } else {
                    return "enlist `" + ((String) literal.getValue2()).replaceAll("'", "");
                }
            } else if (literal.getTypeName() == SqlTypeName.DECIMAL) {
                return literal.getValue();
            } else if (literal.getTypeName() == SqlTypeName.DATE) {
                DateTimeFormatter format = DateTimeFormatter.ofPattern("YYYY.MM.dd").withZone(ZoneId.of("UTC"));
                Calendar date = (Calendar) literal.getValue();
                String dateStr = format.format(date.toInstant());
                return dateStr;
            }
            return literal.getValue2();
        }

        private String translateMatch(RexNode condition) {
            return " " + translateOr(condition);
        }

        private String translateOr(RexNode condition) {
            List<String> list = new ArrayList<String>();
            for (RexNode node : RelOptUtil.disjunctions(condition)) {
                list.add(translateAnd(node));
            }
            switch (list.size()) {
                case 1:
                    return list.get(0);
                default:
//        List<String> ll = Lists.newArrayList();
                    String prefix = "enlist ";
                    String col = null;
                    for (String s : list) {
                        String[] vals = s.split(";");
                        col = vals[1];
                        prefix += vals[2].replace("enlist", "").replaceAll("\\)", "").replaceAll(" ", "");
//            String r = s.split("=")[1].replaceAll(" ","");
//            prefix = s.split("=")[0].replaceAll(" ","");
//            ll.add(r);
                    }
                    return "(enlist (in;" + col + ";" + prefix + "))";
            }
        }

        /**
         * Translates a condition that may be an AND of other conditions. Gathers
         * together conditions that apply to the same field.
         */
        private String translateAnd(RexNode node0) {
            eqMap.clear();
            multimap.clear();
            for (RexNode node : RelOptUtil.conjunctions(node0)) {
                translateMatch2(node.accept(new RexStringShuttle(symbolCols, fieldNames)));
            }
            Map<String, String> predicates = Maps.newHashMap();
            for (Map.Entry<String, RexLiteral> entry : eqMap.entrySet()) {
                multimap.removeAll(entry.getKey());
                predicates.put(entry.getKey(), "(=;`" + entry.getKey() + ";" + literalValue(entry.getValue()) + ")");
            }

            for (Map.Entry<String, Collection<Pair<String, RexLiteral>>> entry
                    : multimap.asMap().entrySet()) {
                for (Pair<String, RexLiteral> s : entry.getValue()) {
                    StringBuffer buffer = new StringBuffer();
                    buffer.append("(");
                    buffer.append(s.left);
                    buffer.append(";");
                    buffer.append("`").append(entry.getKey());
                    buffer.append(";");
                    buffer.append(literalValue(s.right, s.left));
                    buffer.append(")");
                    predicates.put(entry.getKey(), buffer.toString());
                }
            }
            List<String> sortedPredicates = sorted(predicates);
            String allPredicates = Joiner.on(";").join(sortedPredicates);
            return "(" + ((sortedPredicates.size() == 1) ? "enlist " : "") + allPredicates + ")";
        }

        private List<String> sorted(Map<String, String> predicates) {
            List<String> sortedPredicates = Lists.newArrayList();
            if (readDefinition.getPartitionColumnsList() != null) {
                for (String partCol : readDefinition.getPartitionColumnsList()) {
                    String val = predicates.remove(partCol);
                    if (val != null) {
                        sortedPredicates.add(val);
                    }
                }
            }
            if (readDefinition.getSortColumnsList() != null) {
                for (String sortCol : readDefinition.getSortColumnsList()) {
                    String val = predicates.remove(sortCol);
                    if (val != null) {
                        sortedPredicates.add(predicates.get(sortCol));
                    }
                }
            }
            sortedPredicates.addAll(predicates.values());
            return sortedPredicates;
        }

        private void addPredicate(Map<String, String> map, String op, Object v) {
            if (map.containsKey(op) && stronger(op, map.get(op), v)) {
                return;
            }
            map.put(op, v.toString());
        }

        /**
         * Returns whether {@code v0} is a stronger value for operator {@code key}
         * than {@code v1}.
         *
         * <p>For example, {@code stronger("$lt", 100, 200)} returns true, because
         * "&lt; 100" is a more powerful condition than "&lt; 200".
         */
        private boolean stronger(String key, Object v0, Object v1) {
            if ("<".equals(key) || "<=".equals(key)) {
                if (v0 instanceof Number && v1 instanceof Number) {
                    return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
                }
                if (v0 instanceof String && v1 instanceof String) {
                    return v0.toString().compareTo(v1.toString()) < 0;
                }
            }
            if (">".equals(key) || ">=".equals(key)) {
                return stronger("<", v1, v0);
            }
            return false;
        }

        private Void translateMatch2(RexNode node) {
            switch (node.getKind()) {
                case EQUALS:
                    return translateBinary(null, null, (RexCall) node);
                case LESS_THAN:
                    return translateBinary("<", ">", (RexCall) node);
                case LESS_THAN_OR_EQUAL:
                    return translateBinary("<=", ">=", (RexCall) node);
                case NOT_EQUALS:
                    return translateBinary("<>", "<>", (RexCall) node);
                case GREATER_THAN:
                    return translateBinary(">", "<", (RexCall) node);
                case GREATER_THAN_OR_EQUAL:
                    return translateBinary(">=", "<=", (RexCall) node);
                case LIKE:
                    return translateBinary("like", null, (RexCall) node);
                default:
                    throw new AssertionError("cannot translate " + node);
            }
        }

        /**
         * Translates a call to a binary operator, reversing arguments if
         * necessary.
         */
        private Void translateBinary(String op, String rop, RexCall call) {
            final RexNode left = call.operands.get(0);
            final RexNode right = call.operands.get(1);
            boolean b = translateBinary2(op, left, right);
            if (b) {
                return null;
            }
            b = translateBinary2(rop, right, left);
            if (b) {
                return null;
            }
            throw new AssertionError("cannot translate op " + op + " call " + call);
        }

        /**
         * Translates a call to a binary operator. Returns whether successful.
         */
        private boolean translateBinary2(String op, RexNode left, RexNode right) {
            switch (right.getKind()) {
                case LITERAL:
                    break;
                default:
                    return false;
            }
            final RexLiteral rightLiteral = (RexLiteral) right;
            switch (left.getKind()) {
                case INPUT_REF:
                    final RexInputRef left1 = (RexInputRef) left;
                    String name = fieldNames.get(left1.getIndex());
                    translateOp2(op, name, rightLiteral);
                    return true;
                case CAST:
                    return translateBinary2(op, ((RexCall) left).operands.get(0), right);
                case OTHER_FUNCTION:
                    String itemName = RexToKdbColTranslator.isItem((RexCall) left);
                    if (itemName != null) {
                        translateOp2(op, itemName, rightLiteral);
                        return true;
                    }
                    // fall through
                default:
                    return false;
            }
        }

        private void translateOp2(String op, String name, RexLiteral right) {
            if (op == null) {
                // E.g.: {deptno: 100}
                eqMap.put(name, right);
            } else {
                // E.g. {deptno: {$lt: 100}}
                // which may later be combined with other conditions:
                // E.g. {deptno: [$lt: 100, $gt: 50]}
                multimap.put(name, Pair.of(op, right));
            }
        }
    }

    private static final class RexStringShuttle extends RexShuttle {
        private static final RexBuilder BUILDER = new RexBuilder(JavaTypeFactoryImpl.INSTANCE);
        private final Set<String> symbolCols;
        private final List<String> fieldNames;

        private RexStringShuttle(List<String> symbolCols, List<String> fieldNames) {
            this.symbolCols = Sets.newHashSet(symbolCols);
            this.fieldNames = fieldNames;
        }

        @Override
        public RexNode visitCall(RexCall call) {

            if (call.getKind() != SqlKind.EQUALS) {
                return super.visitCall(call);
            } else {
                final RexNode left = call.operands.get(0);
                final RexNode right = call.operands.get(1);
                boolean isSymbol = false;
                boolean isVarChar = false;
                try {
                    isVarChar |= SqlTypeName.VARCHAR.equals(left.getType().getSqlTypeName());
                    isSymbol |= symbolCols.contains(fieldNames.get(((RexInputRef) left).getIndex()));
                } catch (Throwable t) {

                }
                try {
                    isVarChar |= SqlTypeName.VARCHAR.equals(right.getType().getSqlTypeName());
                    isSymbol |= symbolCols.contains(fieldNames.get(((RexInputRef) right).getIndex()));
                } catch (Throwable t) {

                }
                if ((!isSymbol && isVarChar) || (left.getType().getSqlTypeName() == SqlTypeName.CHAR &&
                        right.getType().getSqlTypeName() == SqlTypeName.CHAR)) {
                    return BUILDER.makeCall(SqlStdOperatorTable.LIKE, left, right);
                }
                return super.visitCall(call);
            }
        }
    }

}
