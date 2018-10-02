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
package com.dremio.extras.plugins.kdb.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbIntermediatePrel;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.KdbProject;

/**
 * kdb project pushdown rule
 * <p>
 * This rule starts by finding grabbing any existing projects from the kdb
 * subtree. It then merges the found project with the existing project (if
 * found). From there, it does a rewrite of the expression tree to determine if the particular expression can be pushed down.
 */
public class KdbProjectRule extends RelOptRule {
    private FunctionLookupContext functionLookupContext;

    public KdbProjectRule(FunctionLookupContext functionLookupContext) {
        super(RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(KdbIntermediatePrel.class)), "KdbProjectRule");
        this.functionLookupContext = functionLookupContext;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return true;

    }


    @Override
    public void onMatch(RelOptRuleCall call) {
        final ProjectPrel project = call.rel(0);
        final KdbIntermediatePrel intermediatePrel = call.rel(1);

        final KdbRuleRelVisitor visitor = new ProjectConverterVisitor(project, intermediatePrel.getInput(), intermediatePrel.getTableMetadata(), functionLookupContext).go();
        final KdbPrel newProject = visitor.getConvertedTree();

        call.transformTo(intermediatePrel.withNewInput(newProject));
    }

    class ProjectConverterVisitor extends KdbRuleRelVisitor {
        ProjectConverterVisitor(
                Project project, RelNode input, TableMetadata tableMetadata, FunctionLookupContext functionLookupContext) {
            super(input, tableMetadata, functionLookupContext);
            setProjectExprs(project.getProjects());
            setProjectDataType(project.getRowType());
        }

        @Override
        public void processFilter(KdbFilter filter) {
            setChild(filter);
            setContinueToChildren(false);
        }

        @Override
        public void processProject(KdbProject project) {
            setProjectExprs(RelOptUtil.pushPastProject(getProjectExprs(), project));
            // projectDataType should not be set here, since we want to keep the top project's row type.
        }


    }
}
