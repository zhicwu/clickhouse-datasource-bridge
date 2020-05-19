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

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.vertx.core.json.JsonObject;

public class ClickHouseDataSource implements Closeable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClickHouseDataSource.class);

    private static final String DATASOURCE_TYPE = "general";

    protected static final String CONF_PARAMETERS = "parameters";

    // See all supported values defined in:
    // https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Parsers/IdentifierQuotingStyle.h
    public static final String DEFAULT_QUOTE_IDENTIFIER = "`";

    public static final String CONF_TYPE = "type";

    private final Cache<String, ClickHouseColumnList> columnsCache = Caffeine.newBuilder().maximumSize(100)
            .expireAfterAccess(5, TimeUnit.MINUTES).build();

    private final String id;

    private final JsonObject parameters;

    public static void writeDebugInfo(String dsId, String dsType, String query, QueryParameters parameters,
            ClickHouseResponseWriter writer) {
        ClickHouseBuffer buffer = ClickHouseBuffer.newInstance(query.length() * 2);

        for (String str : new String[] { dsId, dsType, query,
                parameters == null ? null : parameters.toQueryString() }) {
            if (str == null) {
                buffer.writeNull();
            } else {
                buffer.writeNonNull().writeString(str);
            }
        }

        Objects.requireNonNull(writer).write(buffer);
    }

    protected ClickHouseColumnList inferColumns(String schema, String query) {
        return ClickHouseColumnList.DEFAULT_COLUMNS_INFO;
    }

    public ClickHouseDataSource(String id, JsonObject config) {
        this.id = id;

        JsonObject params = config == null ? null : config.getJsonObject(CONF_PARAMETERS);
        this.parameters = params == null ? new JsonObject() : params;
    }

    public final String getId() {
        return this.id;
    }

    public final String getColumns(String schema, String query) {
        final ClickHouseColumnList columns; // = ClickHouseColumnList.DEFAULT_COLUMNS_INFO;

        try {
            columns = columnsCache.get(query, k -> {
                return inferColumns(schema, k);
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to retrieve columns definition", e);
        }

        return columns.toString();
    }

    public final JsonObject getQueryParameters() {
        return this.parameters;
    }

    public final void execute(ClickHouseNamedQuery query, ClickHouseColumnList requestColumns,
            ClickHouseResponseWriter writer) {
        Objects.requireNonNull(query);
        Objects.requireNonNull(requestColumns);

        ClickHouseColumnList allColumns = query.getColumns();
        for (int i = 0; i < requestColumns.size(); i++) {
            ClickHouseColumnInfo r = requestColumns.getColumn(i);
            for (int j = 0; j < allColumns.size(); j++) {
                if (r.getName().equals(allColumns.getColumn(j).getName())) {
                    r.setIndex(j);
                    break;
                }
            }
        }

        execute(query.getQuery(), requestColumns, query.getParameters(), writer);
    }

    @Override
    public void close() throws IOException {
        log.info("Closing datasource[id={}, instance={}]", this.id, this);
    }

    public void execute(String query, ClickHouseColumnList columns, QueryParameters parameters,
            ClickHouseResponseWriter writer) {
        log.info("Executing query:\n{}", query);

        writeDebugInfo(this.id, getType(), query, parameters, writer);
    }

    public String getQuoteIdentifier() {
        return DEFAULT_QUOTE_IDENTIFIER;
    }

    public String getType() {
        return DATASOURCE_TYPE;
    }
}