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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.KdbProject;
import com.dremio.extras.plugins.kdb.rels.RexToKdbColTranslator;
import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * translate project
 */
public class TranslateProject implements Translate {
    private final StringBuffer functionalBuffer;
    private final List<SchemaPath> projectedColumns;
    private final List<KdbColumn> aggregates;
    private final List<KdbColumn> aggCols;
    private final BatchSchema schema;
    private final List<String> groupbyStr;
    private List<String> newGroupbyStr = null;
    private final List<Pair<String, String>> finalProjectedColumns = Lists.newArrayList();
    private final List<KdbProject> projects = Lists.newArrayList();

    public TranslateProject(
            StringBuffer functionalBuffer,
            List<KdbPrel> stack,
            List<SchemaPath> projectedColumns, //projected columns from calcite
            List<KdbColumn> aggregates, //columns being aggregated
            List<KdbColumn> aggCols, //group by columns
            BatchSchema schema,
            String groupbyStr) {

        this.functionalBuffer = functionalBuffer;
        this.projectedColumns = projectedColumns;
        this.aggregates = (aggregates == null) ? Lists.newArrayList() : aggregates;
        this.aggCols = (aggCols == null) ? Lists.newArrayList() : aggCols;
        this.schema = schema;
        this.groupbyStr = Lists.newArrayList((groupbyStr == null ? "" : groupbyStr).split("`")).stream().map(x -> "`" + x).collect(Collectors.toList());
        for (KdbPrel prel : stack) {
            if (prel instanceof KdbProject) {
                projects.add((KdbProject) prel);
            }
        }
    }

    public static String pairsToString(List<Pair<String, String>> projectPairs) {
        String aggProj = "";
        if (projectPairs.size() == 1) {
            aggProj = "(enlist `" + projectPairs.get(0).left + ")!(enlist " + projectPairs.get(0).right + ")";
        } else {
            aggProj = "(`" + Joiner.on("`").join(Pair.left(projectPairs)) + ")!(" + Joiner.on(";").join(Pair.right(projectPairs)) + ")";
        }
        return aggProj;
    }

    static List<String> kdbFieldNames(final RelDataType rowType) {
        return SqlValidatorUtil.uniquify(
                new AbstractList<String>() {
                    @Override
                    public String get(int index) {
                        final String name = rowType.getFieldList().get(index).getName();
                        return name.replaceAll("\\$", "xx_xx");
                    }

                    @Override
                    public int size() {
                        return rowType.getFieldCount();
                    }
                },
                SqlValidatorUtil.EXPR_SUGGESTER, true);
    }

    private String translate() {
        if (projects.isEmpty()) {
            return merge(null, projectedColumns, aggregates);
        }
        if (projects.size() == 1) {
            KdbProject project = projects.get(0);
            List<KdbColumn> translated = implement(project);
            return merge(translated, projectedColumns, aggregates);
        }
        //todo merge projects??
        throw new UnsupportedOperationException();
    }

    private Multimap<String, String> fields(List<KdbColumn> cols, Multimap<String, String> allFields) {
        if (cols == null || cols.isEmpty()) {
            return allFields;
        }
        for (KdbColumn c : cols) {
            if (c.getName() == null || c.getName().equals("0")) {
                continue;
            }
            allFields.put(c.getName(), c.getExpression());
            fields(c.getParents(), allFields);
        }
        return allFields;
    }

    private String merge(
            List<KdbColumn> translated, List<SchemaPath> projectedColumns, List<KdbColumn> aggregates) {

        Multimap<String, String> allFields = HashMultimap.create();
        allFields = fields(translated, allFields);
        Multimap<String, String> aggFields = HashMultimap.create();
        aggFields = fields(aggregates, aggFields);
        Map<String, KdbColumn> projectFields = Maps.newHashMap();
        Set<String> keys = Sets.newHashSet(KdbColumn.names(aggCols));
        if (translated != null) { //translated is the columns that we projected out w/ our KdbProject rules
            for (KdbColumn c : translated) {
                projectFields.put(c.getName(), c);
            }
        } else {
            for (SchemaPath p : projectedColumns) {
                String name = p.getAsNamePart().getName();
                projectFields.put(name, new KdbColumn(name, "`" + name, null, null));
            }
        }
        for (KdbColumn c : aggregates) {

            boolean changed = false;
            for (Map.Entry<String, KdbColumn> kv : projectFields.entrySet()) {
                for (KdbColumn p : kv.getValue().getParents()) {
                    if (c.getName().equals(p.getName())) {
                        //we can collapse a project
                        String expression = kv.getValue().getExpression();
                        kv.getValue().setExpression(expression.replaceAll("`" + c.getName(), c.getExpression()));
                        changed = true;
                    }
                }
                boolean selfReferential = isSelfReferential(c);
                if (c.getName().equals(kv.getValue().getName()) && selfReferential) {
                    //we can collapse a project
                    String expression = c.getExpression();
                    kv.getValue().setExpression(expression.replaceAll("`" + c.getName(), kv.getValue().getExpression()));
                    changed = true;
                }
            }
//      if (projects.containsKey(c.getName())) {
            //projected column is an aggregation, replace w/ aggregate
            if (!changed) {
                projectFields.put(c.getName(), c);
            }
//      }
        }
        for (KdbColumn c : aggregates) {
            for (KdbColumn cc : c.getParents()) {
                if (projectFields.containsKey(cc.getName()) && !keys.contains(cc.getName())) {
                    //this projected column is part of an aggregate and not a agg key...we should prob remove it
                    //this is to match the cases like: select sym,count(distinct time) from trade -> select count distinct time by sym from trade
                    // ...NOT adding a sym or time column (select SYM, TIME, count distinct time by sym from trade)
                    //todo this may not work when we legit want to do select time by sym w/o an aggregate.
                    projectFields.remove(cc.getName());
//          projects.put(c.getName(), c);
                }
            }
        }
        for (KdbColumn c : aggCols) {
            if (projectFields.containsKey(c.getName())) {
                //projected column is an aggregation key remove it
                projectFields.remove(c.getName());
            }
        }

        List<Pair<String, String>> projectPairs = Lists.newArrayList();
        for (Map.Entry<String, KdbColumn> pair : projectFields.entrySet()) {
            projectPairs.add(Pair.of(pair.getKey(), replace(pair.getValue(), allFields, aggFields)));
        }

        String aggProj = pairsToString(projectPairs);
        finalProjectedColumns.addAll(projectPairs);
        finalProjectedColumns.addAll(KdbColumn.toPairList(aggCols));
        return aggProj;
    }

    private boolean isSelfReferential(KdbColumn c) {
        if (c.getExpression().contains("`"+c.getName())) {
            return true;
        }
        return false;
    }

    /**
     * todo sRun separate replacement a la below for each groupby key. Re join as key1;key2 etc
     */
    private String replace(KdbColumn value, Multimap<String, String> allFields, Multimap<String, String> aggFields) {
        for (KdbColumn p : value.getParents()) {
            if (allFields.containsKey(p.getName()) || aggFields.containsKey(p.getName())) {
                Collection<String> fields = allFields.get(p.getName());
                fields.addAll(aggFields.get(p.getName()));
                fields = fields.stream().filter(f -> !f.equals(p.getName())).collect(Collectors.toList());
                if (fields.size() == 1) {
                    String field = fields.iterator().next();
                    if (!p.getName().contains("xx_xx") && !aggFields.containsKey(p.getName())) {
                        continue;
                    }
                    String newExpression = isInt(value.getName(), value.getExpression().replaceAll("`" + p.getName(), field.replace("$", "\\$")));
                    newGroupbyStr = (newGroupbyStr == null ? groupbyStr : newGroupbyStr).stream().map(x -> x.replaceAll("`" + p.getName(), field.replace("$", "\\$"))).collect(Collectors.toList());
                    return newExpression.replaceAll("``", "`");
                }// else {
                //throw new UnsupportedOperationException();
                //}
            }
        }
        return isInt(value);
    }

    private String isInt(KdbColumn expression) {
        return isInt(expression, expression.getExpression());
    }

    private String isInt(KdbColumn expression, String col) {
        for (Field field : schema) {
            if (expression.getName().equals(field.getName()) || expression.getName().equals(field.getName().replace("$", "xx_xx"))) {
                if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.Int) && ((ArrowType.Int) field.getType()).getBitWidth() <= 32) {
                    return "($;\"i\";" + col + ")"; /* Dremio doesnt support short etc so we cast back to int before transfer */
                }
            }
        }
        return col;
    }

    private String isInt(String name, String expression) {
        for (Field field : schema) {
            if (name.equals(field.getName()) || name.equals(field.getName().replace("$", "xx_xx"))) {
                if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.Int) && ((ArrowType.Int) field.getType()).getBitWidth() <= 32) {
                    return "($;\"i\";" + expression + ")"; /* Dremio doesnt support short etc so we cast back to int before transfer */
                }
                if (field.getType().getTypeID().equals(ArrowType.ArrowTypeID.Int) && ((ArrowType.Int) field.getType()).getBitWidth() <= 64) {
                    return "($;\"j\";" + expression + ")"; /* Dremio doesnt support short etc so we cast back to int before transfer */
                }
            }
        }
        return expression;
    }

    private static final Joiner JOINER = Joiner.on(";");

    @Override
    public String go() {
        functionalBuffer.append(translate());
        List<String> finalGroupbyStr = groupbyStr.stream().filter(x -> !x.equals("`")).collect(Collectors.toList());
        List<String> finalNewGroupbyStr = (newGroupbyStr == null) ? null : newGroupbyStr.stream().filter(x -> !x.equals("`")).collect(Collectors.toList());
        String agg = "0b";
        if (finalGroupbyStr.isEmpty()) {
            return agg;
        }
        if (finalGroupbyStr.size() == 1 && finalGroupbyStr.get(0).isEmpty()) {
            return agg;
        }
        if (finalGroupbyStr.size() == 1 && (finalGroupbyStr.get(0).equals("0b") || finalGroupbyStr.get(0).equals("`0b"))) {
            return agg;
        }
        if (finalNewGroupbyStr == null) {
            String joinOne = joinOne(finalGroupbyStr, finalGroupbyStr);
            if (joinOne != null) {
                return joinOne;
            }
            String join = JOINER.join(finalGroupbyStr);
            return "(" + join + ")!(" + join + ")";
        }
        String joinOne = joinOne(finalGroupbyStr, finalNewGroupbyStr);
        if (joinOne != null) {
            return joinOne;
        }
        String join = JOINER.join(finalGroupbyStr);
        String newJoin = JOINER.join(finalNewGroupbyStr);
        return "(" + join + ")!(" + newJoin + ")";
    }

    private String joinOne(List<String> finalGroupbyStr, List<String> finalNewGroupbyStr) {
        if (finalGroupbyStr.size() == 2 && (finalGroupbyStr.get(0).equals("`enlist ") || finalGroupbyStr.get(0).equals("enlist "))) {
            return "(enlist " + finalGroupbyStr.get(1) + ")!(enlist " + finalNewGroupbyStr.get(1) + ")";
        }
        return null;
    }

    private List<KdbColumn> implement(KdbProject project) {
//    implementor.visitChild(0, getInput());

        final RexToKdbColTranslator translator =
                new RexToKdbColTranslator(
                        (JavaTypeFactory) project.getCluster().getTypeFactory(),
                        kdbFieldNames(project.getInput().getRowType()));
        List<KdbColumn> colList = Lists.newArrayList();
        for (Pair<RexNode, String> pair : project.getNamedProjects()) {
            final String name = pair.right;
            if ("DUMMY".equals(name)) {
                continue;
            }
            final KdbColumn expr = pair.left.accept(translator);
            ArrayList<KdbColumn> parents = Lists.newArrayList(expr.getParents());
            parents.add(expr);
            colList.add(new KdbColumn(name.replace("$", "xx_xx"), expr.getExpression(), parents, project.getRowType().getField(name, true, true)));
        }
        if (colList.isEmpty()) {
            return Lists.newArrayList();
        }
        return colList;
    }

    public List<Pair<String, String>> getProjectedColumns() {
        return finalProjectedColumns;
    }
}
