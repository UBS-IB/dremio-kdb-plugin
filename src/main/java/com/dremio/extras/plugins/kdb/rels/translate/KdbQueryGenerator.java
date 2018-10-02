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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.util.Pair;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.TableMetadata;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbLimit;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.KdbProject;
import com.dremio.extras.plugins.kdb.rels.KdbSort;
import com.dremio.extras.plugins.kdb.rels.KdbTableMetaData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * generator class to do Prel -> q translation
 */
public class KdbQueryGenerator {
    private final KdbPrel prel;
    private final List<SchemaPath> projectedColumns;
    private final TableMetadata tableMetadata;
    private final FunctionLookupContext functionLookupContext;
    private final RelOptCluster cluster;
    private final BatchSchema schema;
    private List<KdbColumn> aggregates = Lists.newArrayList();
    private List<KdbColumn> aggCols = Lists.newArrayList();
    private List<Pair<String, String>> finalProjectedColumns = Lists.newArrayList();
    private List<SchemaPath> transformedProjectedColumns;
    private TableMetadata transformedTableMetadata;

    public KdbQueryGenerator(
            KdbPrel prel, List<SchemaPath> projectedColumns, TableMetadata tableMetadata, FunctionLookupContext functionLookupContext, RelOptCluster cluster,
            BatchSchema schema) {

        this.prel = prel;
        this.projectedColumns = projectedColumns;
        this.tableMetadata = tableMetadata;
        this.functionLookupContext = functionLookupContext;
        this.cluster = cluster;
        this.schema = schema;
        for (SchemaPath p : projectedColumns) {
            String name = p.getAsNamePart().getName();
            finalProjectedColumns.add(Pair.of(name, name));
        }
    }

    public String generate() {
        KdbQuery query = new KdbQuery(tableMetadata.getReadDefinition(), schema);
        KdbPrel rel = (KdbPrel) prel.accept(new MoreRelOptUtil.SubsetRemover(false));
        KdbQueryParameters queryParameters = rel.accept(query, new KdbQueryParameters());
        return generateQuery(queryParameters);
    }

    private String generateQuery(KdbQueryParameters parameters) {
        SimplifyAgg simplifyAgg = new SimplifyAgg(parameters, cluster.getTypeFactory(), tableMetadata.getReadDefinition(), functionLookupContext, cluster);
        parameters = simplifyAgg.go();
        StringBuffer functionalBuffer = new StringBuffer();
        functionalBuffer.append("?[");
        functionalBuffer.append(getTable(parameters));
        functionalBuffer.append(";");
        functionalBuffer.append(getWhere(parameters));
        functionalBuffer.append(";");
        String groupbyStr = getGroupby(parameters);
        Pair<String, String> projectionAggStr = getProjection(parameters, schema, groupbyStr);
        functionalBuffer.append(projectionAggStr.right);
        functionalBuffer.append(";");
        functionalBuffer.append(projectionAggStr.left);
        functionalBuffer.append("]");
        functionalBuffer = appendSort(functionalBuffer, parameters);
        functionalBuffer = appendLimit(functionalBuffer, parameters);
        String query = functionalBuffer.toString();
        transformedProjectedColumns = transformProjectedColumns();
        transformedTableMetadata = transformTableMetadata();
        return query;
    }

    private TableMetadata transformTableMetadata() {
        TableMetadata newTableMetadata = new KdbTableMetaData(this.tableMetadata, mergeSchema(Sets.newHashSet(Pair.left(finalProjectedColumns)), schema));
        return newTableMetadata;
    }

    private List<SchemaPath> transformProjectedColumns() {
        List<SchemaPath> newProjectedColumns = merge(finalProjectedColumns);
        return newProjectedColumns;
    }

    private BatchSchema mergeSchema(Set<String> columns, BatchSchema original) {
        SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
        for (Field field : original) {
            if (columns.contains(field.getName()) || columns.contains(field.getName().replace("$", "xx_xx"))) {
                schemaBuilder.addField(field);
            }
        }
        schemaBuilder.setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
        return schemaBuilder.build();
    }

    private List<SchemaPath> merge(List<Pair<String, String>> newFields) {
        ImmutableList.Builder<SchemaPath> builder = ImmutableList.builder();
        for (String f : Pair.left(newFields)) {
            builder.add(SchemaPath.getSimplePath(f.replace("xx_xx", "$")));
        }
        return builder.build();
    }

    public TableMetadata tableMetadata() {
        return transformedTableMetadata;
    }

    public List<SchemaPath> projectedColumns() {
        return transformedProjectedColumns;
    }

    private StringBuffer appendLimit(StringBuffer functionalBuffer, KdbQueryParameters parameters) {
        KdbLimit filter = parameters.getLimit();
        if (filter == null) {
            return functionalBuffer;
        }

        TranslateLimit limit = new TranslateLimit(functionalBuffer, ImmutableList.of(filter), functionLookupContext, cluster.getRexBuilder());
        limit.go();
        return limit.buffer();
    }

    private StringBuffer appendSort(StringBuffer functionalBuffer, KdbQueryParameters parameters) {
        KdbSort filter = parameters.getSort();
        if (filter == null) {
            return functionalBuffer;
        }

        TranslateSort limit = new TranslateSort(functionalBuffer, ImmutableList.of(filter), Pair.left(finalProjectedColumns));
        limit.go();
        return limit.buffer();
    }

    private String getTable(KdbQueryParameters parameters) {
        if (parameters.scanTable() != null) {
            return generateQuery(parameters.scanTable());
        } else {
//      assert parameters.scan() != null;
            return tableMetadata.getName().getName();
        }
    }

    private Pair<String, String> getProjection(KdbQueryParameters parameters, BatchSchema schema, String groupbyStr) {
        KdbProject filter = parameters.getProject();
//    if (filter == null) {
//      List<Pair<String, String>> projectPairs = projectedColumns.stream().map(x->Pair.of(x.getAsNamePart().getName(), "`" + x.getAsNamePart().getName())).collect(Collectors.toList());
//      return TranslateProject.pairsToString(projectPairs);
//    }
        StringBuffer buffer = new StringBuffer();
        TranslateProject project = new TranslateProject(buffer, (filter == null) ? ImmutableList.of() : ImmutableList.of(filter), projectedColumns, aggregates, aggCols, schema, groupbyStr);
        String newAggString = project.go();
        finalProjectedColumns = project.getProjectedColumns();
        return Pair.of(buffer.toString(), newAggString);
    }

    private String getGroupby(KdbQueryParameters parameters) {
        KdbAggregate filter = parameters.getAggregate();
        if (filter == null) {
            return "0b";
        }
        TranslateAgg agg = new TranslateAgg(ImmutableList.of(filter), tableMetadata.getReadDefinition());
        String aggStr = agg.go();
        aggregates = agg.getProjectedColumns();
        aggCols = agg.getAggregateColumns();
        return aggStr;
    }

    private String getWhere(KdbQueryParameters parameters) {
        KdbFilter filter = parameters.getFilter();
        if (filter == null) {
            return "()";
        }
        StringBuffer buffer = new StringBuffer();
        new TranslateWhere(buffer, ImmutableList.of(filter), tableMetadata.getReadDefinition()).go();
        return buffer.toString();
    }


}
