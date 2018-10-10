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
package com.dremio.extras.plugins.kdb.rels;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.extras.plugins.kdb.rels.functions.BinaryFunction;
import com.dremio.extras.plugins.kdb.rels.functions.CaseKdbFunction;
import com.dremio.extras.plugins.kdb.rels.functions.DateAddFunction;
import com.dremio.extras.plugins.kdb.rels.functions.KdbFunction;
import com.dremio.extras.plugins.kdb.rels.functions.Log10Function;
import com.dremio.extras.plugins.kdb.rels.functions.NoArgKdbFunction;
import com.dremio.extras.plugins.kdb.rels.functions.UnaryFunction;
import com.dremio.extras.plugins.kdb.rels.functions.XbarFunction;
import com.dremio.extras.plugins.kdb.rels.translate.KdbColumn;
import com.dremio.extras.plugins.kdb.rels.translate.TranslateWhere;
import com.google.common.collect.ImmutableMap;


/**
 * Translator from {@link RexNode} to strings in KDB's expression
 * language.
 */
public class RexToKdbColTranslator extends RexVisitorImpl<KdbColumn> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RexToKdbColTranslator.class);
    private static final Map<SqlOperator, KdbFunction> KDB_OPERATORS =
            new HashMap<SqlOperator, KdbFunction>();

    static {
        // Arithmetic
        KDB_OPERATORS.put(SqlStdOperatorTable.DIVIDE, new BinaryFunction("%"));
        KDB_OPERATORS.put(SqlStdOperatorTable.MULTIPLY, new BinaryFunction("*"));
        KDB_OPERATORS.put(SqlStdOperatorTable.MOD, new BinaryFunction("mod"));
        KDB_OPERATORS.put(SqlStdOperatorTable.PLUS, new BinaryFunction("+"));
        KDB_OPERATORS.put(SqlStdOperatorTable.MINUS, new BinaryFunction("-"));
        // Boolean
        KDB_OPERATORS.put(SqlStdOperatorTable.AND, new BinaryFunction("and"));
        KDB_OPERATORS.put(SqlStdOperatorTable.OR, new BinaryFunction("or"));
        KDB_OPERATORS.put(SqlStdOperatorTable.NOT, new BinaryFunction("not"));
        // Comparison
        KDB_OPERATORS.put(SqlStdOperatorTable.EQUALS, new BinaryFunction("="));
        KDB_OPERATORS.put(SqlStdOperatorTable.NOT_EQUALS, new BinaryFunction("<>"));
        KDB_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN, new BinaryFunction(">"));
        KDB_OPERATORS.put(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, new BinaryFunction(">="));
        KDB_OPERATORS.put(SqlStdOperatorTable.LESS_THAN, new BinaryFunction("<"));
        KDB_OPERATORS.put(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, new BinaryFunction("<="));
        KDB_OPERATORS.put(SqlStdOperatorTable.CURRENT_DATE, new NoArgKdbFunction(".z.d"));
        KDB_OPERATORS.put(SqlStdOperatorTable.CASE, new CaseKdbFunction());
        KDB_OPERATORS.put(SqlStdOperatorTable.ABS, new UnaryFunction("abs"));
        KDB_OPERATORS.put(SqlStdOperatorTable.ACOS, new UnaryFunction("acos"));
        KDB_OPERATORS.put(SqlStdOperatorTable.ATAN, new UnaryFunction("atan"));
        KDB_OPERATORS.put(SqlStdOperatorTable.ASIN, new UnaryFunction("asin"));
        KDB_OPERATORS.put(SqlStdOperatorTable.COS, new UnaryFunction("cos"));
        KDB_OPERATORS.put(SqlStdOperatorTable.SIN, new UnaryFunction("sin"));
        KDB_OPERATORS.put(SqlStdOperatorTable.TAN, new UnaryFunction("tan"));
        KDB_OPERATORS.put(SqlStdOperatorTable.EXP, new UnaryFunction("exp"));
        KDB_OPERATORS.put(SqlStdOperatorTable.SQRT, new UnaryFunction("sqrt"));
        KDB_OPERATORS.put(SqlStdOperatorTable.LN, new UnaryFunction("log"));
        KDB_OPERATORS.put(SqlStdOperatorTable.LOG10, new Log10Function("xlog"));
        KDB_OPERATORS.put(SqlStdOperatorTable.POWER, new BinaryFunction("xexp"));


    }

    private static final Map<String, KdbFunction> KDB_COMPLEX_OPERATORS = ImmutableMap.<String, KdbFunction>builder()
            .put("date_trunc", new XbarFunction())
            .put("date_add", new DateAddFunction(true))
            .put("date_sub", new DateAddFunction(false))
            .put("log", new UnaryFunction("log"))
            .build();

    private final JavaTypeFactory typeFactory;
    private final List<String> inFields;

    public RexToKdbColTranslator(
            JavaTypeFactory typeFactory, List<String> inFields) {
        super(true);
        this.typeFactory = typeFactory;
        this.inFields = inFields;
    }

    public static String isItem(RexCall call) {
        if (call.getOperator() != SqlStdOperatorTable.ITEM) {
            return null;
        }
        final RexNode op0 = call.operands.get(0);
        final RexNode op1 = call.operands.get(1);
        if (op0 instanceof RexInputRef
                && ((RexInputRef) op0).getIndex() == 0
                && op1 instanceof RexLiteral
                && ((RexLiteral) op1).getValue2() instanceof String) {
            return (String) ((RexLiteral) op1).getValue2();
        }
        return null;
    }

    @Override
    public KdbColumn visitLiteral(RexLiteral literal) {
        if (literal.getValue() == null) {
            return KdbColumn.name("null", null);
        }
        Object newLiteralStr = TranslateWhere.Translator.literalValue(literal);
        Expression newLiteral = Expressions.constant(newLiteralStr);
        return KdbColumn.name(stripQuotes(newLiteral.toString()), canonicalize(newLiteral, literal.getTypeName()));
    }

    private String canonicalize(Expression literal, SqlTypeName typeName) {
        if (literal instanceof ConstantExpression) {
            Type type = literal.getType();
            if (type == int.class) {
                return literal.toString() + "i";
            } else if (typeName.equals(SqlTypeName.DATE)) {
                return stripQuotes(literal.toString());
            } else if (type == String.class) {
                return /*"enlist `" +*/ stripQuotes(literal.toString());
            } else if (type == BigDecimal.class) {
                return ((ConstantExpression) literal).value.toString();
            }
        }
        return literal.toString();
    }

    @Override
    public KdbColumn visitInputRef(RexInputRef inputRef) {
        String name = inFields.get(inputRef.getIndex());
        return KdbColumn.name(name, "`" + name, new RelDataTypeFieldImpl(name, inputRef.getIndex(), inputRef.getType()));
    }

    @Override
    public KdbColumn visitCall(RexCall call) {
        String name = isItem(call);
        if (name != null) {
            return KdbColumn.name(name, "'$" + name + "'");
        }
        final List<KdbColumn> strings = visitList(call.operands);
        if (call.getKind() == SqlKind.CAST) {
            return strings.get(0); //todo being lazy w/ cast
        }
        KdbFunction stdOperator = KDB_OPERATORS.get(call.getOperator());
        if (stdOperator != null) {
            return stdOperator.render(strings);
        }
        KdbFunction complexOperator = KDB_COMPLEX_OPERATORS.get(call.getOperator().getName().toLowerCase());
        if (complexOperator != null) {
            return complexOperator.render(strings);
        }

        throw new IllegalArgumentException("Translation of " + call.toString()
                + " is not supported by KdbProject");
    }

    private String stripQuotes(String s) {
        return s.startsWith("\"") && s.endsWith("\"")
                ? s.substring(1, s.length() - 1)
                : s;
    }

    public List<KdbColumn> visitList(List<RexNode> list) {
        final List<KdbColumn> strings = new ArrayList<KdbColumn>();
        for (RexNode node : list) {
            strings.add(node.accept(this));
        }
        return strings;
    }
}
