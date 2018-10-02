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

import java.math.BigDecimal;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.extras.plugins.kdb.rels.KdbLimit;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * translate limits
 */
public class TranslateLimit implements Translate {
    private final List<KdbLimit> limits = Lists.newArrayList();
    private final RexBuilder rexBuilder;
    private final BiFunction<BigDecimal, BigDecimal, BigDecimal> offsetMatcher = (current, newVal) -> {
        if (current == null) {
            return newVal;
        }
        if (newVal == null) {
            return current;
        }
        return current.min(newVal);
    };
    private final BiFunction<BigDecimal, BigDecimal, BigDecimal> fetchMatcher = (current, newVal) -> {
        if (current == null) {
            return newVal;
        }
        if (newVal == null) {
            return current;
        }
        return current.min(newVal);
    };
    private StringBuffer functionalBuffer;
    private FunctionLookupContext functionLookupContext;

    public TranslateLimit(
            StringBuffer functionalBuffer, List<KdbPrel> stack, FunctionLookupContext functionLookupContext, RexBuilder rexBuilder) {

        this.functionalBuffer = functionalBuffer;
        this.functionLookupContext = functionLookupContext;
        this.rexBuilder = rexBuilder;
        for (KdbPrel prel : stack) {
            if (prel instanceof KdbLimit) {
                limits.add((KdbLimit) prel);
            }
        }
    }

    public List<String> implement(KdbLimit limit) {
        List<String> implementor = Lists.newArrayList();
        if (limit.getOffset() != null && limit.getFetch() != null) {
            Long offsetVal = (Long) ((RexLiteral) limit.getOffset()).getValue2();
            Long fetchVal = (Long) ((RexLiteral) limit.getFetch()).getValue2();
            if (offsetVal > 0) {
                implementor.add(" i > " + offsetVal);
            }

            implementor.add(" i < " + (offsetVal + fetchVal));

        } else {
            if (limit.getOffset() != null) {
                Long offsetVal = (Long) ((RexLiteral) limit.getOffset()).getValue2();
                if (offsetVal > 0) {
                    implementor.add(" i > " + ((RexLiteral) limit.getOffset()).getValue());
                }
            }
            if (limit.getFetch() != null) {
                implementor.add(" i < " + ((RexLiteral) limit.getFetch()).getValue());
            }
        }
        return implementor;
    }

    private StringBuffer addLimit(KdbLimit limit) {
        String limitStr = Joiner.on(",").join(implement(limit));
        if (!limitStr.isEmpty()) {
            StringBuffer newBuffer = new StringBuffer();
            newBuffer.append("select from (");
            newBuffer.append(functionalBuffer);
            newBuffer.append(" ) where ");
            newBuffer.append(limitStr);
            return newBuffer;
        }
        return functionalBuffer;
    }

    @Override
    public String go() {
        if (limits.isEmpty()) {
            return "";
        }
        if (limits.size() == 1) {
            KdbLimit limit = limits.get(0);
            functionalBuffer = addLimit(limit);
            return functionalBuffer.toString();
        }

        KdbLimit finalLimit = null;
        RexNode offset = null;
        RexNode fetch = null;
        for (KdbLimit limit : limits) {
            offset = merge(offset, limit.getOffset(), offsetMatcher);
            fetch = merge(fetch, limit.getFetch(), fetchMatcher);
            finalLimit = new KdbLimit(limit.getCluster(), limit.getTraitSet(), limit.getInput(), offset, fetch, limit.getSchema(functionLookupContext), limit.projectedColumns());
        }
        functionalBuffer = addLimit(finalLimit);
        return functionalBuffer.toString();
    }

    private RexNode merge(RexNode currentRex, RexNode newRex, BiFunction<BigDecimal, BigDecimal, BigDecimal> matcher) {
        try {
            if (newRex instanceof RexLiteral) {
                if (currentRex == null) {
                    return newRex;
                }
                BigDecimal newValue = (BigDecimal) ((RexLiteral) newRex).getValue();
                BigDecimal currentValue = (BigDecimal) ((RexLiteral) currentRex).getValue();

                BigDecimal value = matcher.apply(currentValue, newValue);
                RexLiteral combined = rexBuilder.makeBigintLiteral(value);
                return combined;
            }
            if (newRex == null) {
                return currentRex;
            }
            throw new UnsupportedOperationException();
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (Throwable t) {
            return null;
        }
    }


    @Override
    public StringBuffer buffer() {
        return functionalBuffer;
    }
}
