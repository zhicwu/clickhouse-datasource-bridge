/**
 * Copyright (C) 2019-2020, Zhichun Wu
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.clickhouse.bridge.core;

import java.util.Objects;

import io.vertx.core.json.JsonObject;

public class ClickHouseNamedQuery {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClickHouseNamedQuery.class);

    private static final String CONF_QUERY = "query";
    private static final String CONF_COLUMNS = "columns";
    private static final String CONF_PARAMETERS = "parameters";

    private final String id;
    private final String digest;
    private final String query;
    private final ClickHouseColumnList columns;

    private final QueryParameters parameters;

    public ClickHouseNamedQuery(String id, JsonObject config) {
        Objects.requireNonNull(config);

        this.id = id;
        this.digest = ClickHouseUtils.digest(config);

        String namedQuery = config.getString(CONF_QUERY);
        Objects.requireNonNull(namedQuery);

        this.query = namedQuery;
        this.columns = ClickHouseColumnList.fromJson(config.getJsonArray(CONF_COLUMNS));
        this.parameters = new QueryParameters(config.getJsonObject(CONF_PARAMETERS));
    }

    public String getId() {
        return this.id;
    }

    public String getQuery() {
        return this.query;
    }

    public boolean hasColumn() {
        return this.columns != null && this.columns.hasColumn();
    }

    public ClickHouseColumnList getColumns() {
        return this.columns;
    }

    public QueryParameters getParameters() {
        return this.parameters;
    }

    public final boolean isDifferentFrom(JsonObject newConfig) {
        String newDigest = ClickHouseUtils.digest(newConfig == null ? null : newConfig.encode());
        boolean isDifferent = this.digest == null || this.digest.length() == 0 || !this.digest.equals(newDigest);
        if (isDifferent) {
            log.info("Query configuration of [{}] is changed from [{}] to [{}]", this.id, digest, newDigest);
        } else {
            log.debug("Query configuration of [{}] remains the same", this.id);
        }

        return isDifferent;
    }
}