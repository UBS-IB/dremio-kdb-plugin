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
package com.dremio.extras.plugins.kdb.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.common.MoreRelOptUtil.SubsetRemover;
import com.dremio.exec.store.TableMetadata;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.KdbProject;

/**
 * Visits the kdb subtree and collapses the tree.
 */
public abstract class KdbRuleRelVisitor extends RelVisitor {
    private final RelNode input;
    private final TableMetadata tableMetadata;
    private final FunctionLookupContext functionLookupContext;
    private RexNode filterExprs = null;
    private List<RexNode> projectExprs = null;
    private RelDataType projectDataType = null;
    private RelNode child = null;
    private List<KdbPrel> parents = new ArrayList<>();
    private boolean continueToChildren = true;

    public KdbRuleRelVisitor(
            RelNode input, TableMetadata tableMetadata, FunctionLookupContext functionLookupContext) {
        this.input = input;
        this.tableMetadata = tableMetadata;
        this.functionLookupContext = functionLookupContext;
    }

    public abstract void processFilter(KdbFilter filter);

    public abstract void processProject(KdbProject project);

    public KdbRuleRelVisitor go() {
        go(input.accept(new SubsetRemover(false)));
        assert child != null;
        return this;
    }

    public List<RexNode> getProjectExprs() {
        return projectExprs;
    }

    @Override
    public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof KdbFilter) {
            processFilter((KdbFilter) node);
        } else if (node instanceof KdbProject) {
            processProject((KdbProject) node);
        } else {
            child = node;
            continueToChildren = false;
        }

        if (continueToChildren) {
            super.visit(node, ordinal, parent);
        }
    }

    public KdbPrel getConvertedTree() {
        KdbPrel subTree = (KdbPrel) this.child;


        if (filterExprs != null) {
            subTree = new KdbFilter(subTree.getCluster(), subTree.getTraitSet(), subTree, filterExprs, subTree.getSchema(functionLookupContext), subTree.projectedColumns(), tableMetadata);
        }

        if (projectExprs != null) {
            subTree = new KdbProject(subTree.getCluster(), subTree.getTraitSet(), subTree, projectExprs, projectDataType, subTree.getSchema(functionLookupContext), subTree.projectedColumns());
        }

        if (parents != null && !parents.isEmpty()) {
            ListIterator<KdbPrel> iterator = parents.listIterator(parents.size());
            while (iterator.hasPrevious()) {
                final KdbPrel parent = iterator.previous();
                subTree = (KdbPrel) parent.copy(parent.getTraitSet(), Collections.singletonList((RelNode) subTree));
            }
        }

        return subTree;
    }

    public RexNode getFilterExprs() {
        return filterExprs;
    }

    public void setProjectExprs(List<RexNode> projectExprs) {
        this.projectExprs = projectExprs;
    }

    void setContinueToChildren(boolean b) {
        continueToChildren = b;
    }

    void setChild(KdbFilter filter) {
        child = filter;
    }

    public void setProjectDataType(RelDataType projectDataType) {
        this.projectDataType = projectDataType;
    }
}
