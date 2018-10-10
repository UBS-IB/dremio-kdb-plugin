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

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlDialect;

import com.dremio.exec.planner.common.LimitRelBase;
import com.dremio.exec.planner.common.ScanRelBase;

/**
 * Rel -> SQL for logging purposes
 */
public class KdbRelToSqlConverter extends RelToSqlConverter {
    public KdbRelToSqlConverter(SqlDialect dialect) {
        super(dialect);
    }

    @Override
    public Result visit(RelNode e) {
        if (e instanceof RelSubset) {
            return visit((RelSubset) e);
        }
        if (e instanceof ScanRelBase) {
            return visit((ScanRelBase) e);
        }
        if (e instanceof KdbSort) {
            return visit((KdbSort) e);
        }
        if (e instanceof LimitRelBase) {
            return visit((LimitRelBase) e);
        }
        if (e instanceof KdbAggregate) {
            return visit((KdbAggregate) e);
        }
        if (e instanceof KdbFilter) {
            return visit((Filter) e);
        }
        return super.visit(e);
    }

    public Result visit(RelSubset e) {
        Result x = this.visitChild(0, e.getBest());
        return x;
    }

    public Result visit(ScanRelBase e) {
        return visit((TableScan) e);
    }

    public Result visit(KdbAggregate e) {
        return visit((Aggregate) e);
    }

    public Result visit(KdbIntermediateScanPrel e) {
        return visit((TableScan) e);
    }

    public Result visit(KdbSort e) {
        return visit((Sort) e);
    }


    public Result visit(LimitRelBase e) {
        Result x = this.visitChild(0, e.getInput());
        Builder builder;
        if (e.getFetch() != null) {
            builder = x.builder(e, new Clause[]{Clause.FETCH});
            builder.setFetch(builder.context.toSql((RexProgram) null, e.getFetch()));
            x = builder.result();
        }

        if (e.getOffset() != null) {

            builder = x.builder(e, new Clause[]{Clause.OFFSET});
            builder.setOffset(builder.context.toSql((RexProgram) null, e.getOffset()));
            x = builder.result();
        }
        return x;
    }
}
