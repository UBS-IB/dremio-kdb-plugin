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

import com.dremio.exec.planner.physical.ExchangePrel;
import com.dremio.exec.planner.physical.JoinPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.ScreenPrel;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.extras.plugins.kdb.rels.KdbAggregate;
import com.dremio.extras.plugins.kdb.rels.KdbFilter;
import com.dremio.extras.plugins.kdb.rels.KdbLimit;
import com.dremio.extras.plugins.kdb.rels.KdbProject;
import com.dremio.extras.plugins.kdb.rels.KdbSort;

/**
 * visitor to generate kdb query info
 */
public interface KdbPrelVisitor {

    public KdbQueryParameters visitExchange(ExchangePrel prel, KdbQueryParameters value);

    public KdbQueryParameters visitScreen(ScreenPrel prel, KdbQueryParameters value);

    public KdbQueryParameters visitWriter(WriterPrel prel, KdbQueryParameters value);

    public KdbQueryParameters visitLeaf(LeafPrel prel, KdbQueryParameters value);

    public KdbQueryParameters visitJoin(JoinPrel prel, KdbQueryParameters value);

    public KdbQueryParameters visitProject(KdbProject prel, KdbQueryParameters value);

    public KdbQueryParameters visitAggregate(KdbAggregate prel, KdbQueryParameters value);

    public KdbQueryParameters visitLimit(KdbLimit prel, KdbQueryParameters value);

    public KdbQueryParameters visitSort(KdbSort prel, KdbQueryParameters value);

    public KdbQueryParameters visitFilter(KdbFilter prel, KdbQueryParameters value);

    public KdbQueryParameters visitPrel(Prel prel, KdbQueryParameters value);

}
