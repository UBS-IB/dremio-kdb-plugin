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

import com.dremio.exec.planner.physical.Prel;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbLimit;
import com.dremio.extras.plugins.kdb.rels.KdbPrel;
import com.dremio.extras.plugins.kdb.rels.KdbProject;
import com.dremio.extras.plugins.kdb.rels.KdbSort;

/**
 * beam for query structure
 */
public class KdbQueryParameters {
    private KdbPrelContainer scan = new KdbPrelContainer(null, null);
    private KdbFilter filter = null;
    private KdbProject project = null;
    private KdbAggregate aggregate = null;
    private KdbSort sort = null;
    private KdbLimit limit = null;


    public KdbPrel scan() {
        return scan.scan;
    }

    public KdbQueryParameters scanTable() {
        return scan.query;
    }

    public KdbLimit getLimit() {
        return limit;
    }

    public void setLimit(KdbLimit prel) {
        this.limit = prel;
    }

    public KdbSort getSort() {
        return sort;
    }

    public void setSort(KdbSort sort) {
        this.sort = sort;
    }

    public KdbPrelContainer getScan() {
        return this.scan;
    }

    public void setScan(Prel prel) {
        this.scan = new KdbPrelContainer(null, (KdbPrel) prel);
    }

    public void setScan(KdbQueryParameters prel) {
        this.scan = new KdbPrelContainer(prel, null);
    }

    public void setScan(KdbPrelContainer scan) {
        this.scan = scan;
    }

    public KdbFilter getFilter() {
        return filter;
    }

    public void setFilter(KdbFilter filter) {
        if (this.filter != null) {
            throw new UnsupportedOperationException();
        }
        this.filter = filter;
    }

    public KdbProject getProject() {
        return project;
    }

    public void setProject(KdbProject project) {
        if (this.project != null) {
            KdbQueryParameters parameters = new KdbQueryParameters();
            parameters.setScan(this.scan);
            parameters.setProject(this.project);
            this.project = project;
            this.scan = new KdbPrelContainer(parameters, null);
//      parameters.setFilter(this.filter);
//      this.filter = null;
//      parameters.setSort(this.sort);
//      this.sort = null;
//      parameters.setLimit(this.limit);
//      this.limit = null;
//      parameters.setAggregate(this.aggregate);
//      this.aggregate = null;

        }
        this.project = project;
    }

    public KdbAggregate getAggregate() {
        return aggregate;
    }

    public void setAggregate(KdbAggregate aggregate) {
        this.aggregate = aggregate;
    }

    @Override
    public String toString() {
        return "KdbQueryParameters{" + "scan=" + scan + ", filter=" + filter + ", project=" + project + ", aggregate=" + aggregate + ", sort=" + sort
                + ", limit=" + limit + '}';
    }

    static final class KdbPrelContainer {
        private final KdbQueryParameters query;
        private final KdbPrel scan;

        private KdbPrelContainer(KdbQueryParameters query, KdbPrel scan) {
            this.query = query;
            this.scan = scan;
        }

        @Override
        public String toString() {
            return "KdbPrelContainer{" + "query=" + query + ", scan=" + scan + '}';
        }

        public KdbQueryParameters getQuery() {
            return query;
        }

        public KdbPrel getScan() {
            return scan;
        }
    }
}
