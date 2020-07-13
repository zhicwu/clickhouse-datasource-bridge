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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static com.github.clickhouse.bridge.core.ClickHouseDataType.*;

public class ClickHouseDataSource implements Closeable {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClickHouseDataSource.class);

    private static final String DATASOURCE_TYPE = "general";

    protected static final String CONF_CACHE = "cache";
    protected static final String CONF_SIZE = "size";
    protected static final String CONF_EXPIRATION = "expiration";

    protected static final String CONF_COLUMNS = "columns";
    protected static final String CONF_DEFAULTS = "defaults";
    protected static final String CONF_PARAMETERS = "parameters";

    // See all supported values defined in:
    // https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Parsers/IdentifierQuotingStyle.h
    public static final String DEFAULT_QUOTE_IDENTIFIER = "`";

    public static final String CONF_SCHEMA = "$schema";
    public static final String CONF_TYPE = "type";
    public static final String CONF_TIMEZONE = "timezone";

    private static final String QUERY_FILE_EXT = ".query";

    private final Cache<String, ClickHouseColumnList> columnsCache;

    private final String id;

    private final String digest;

    private final TimeZone timezone;
    private final List<ClickHouseColumnInfo> customColumns;
    private final DefaultValues defaultValues;
    private final QueryParameters queryParameters;

    public static void writeDebugInfo(String dsId, String dsType, ClickHouseColumnList metaData, String query,
            QueryParameters parameters, ClickHouseResponseWriter writer) {
        if (metaData == null) {
            metaData = new ClickHouseColumnList();
        }

        ClickHouseBuffer buffer = ClickHouseBuffer.newInstance(query.length() * 2);

        for (String str : new String[] { dsId, dsType, metaData.toJsonString(query), query,
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

    protected boolean isSavedQuery(String file) {
        return Objects.requireNonNull(file).endsWith(QUERY_FILE_EXT);
    }

    public ClickHouseDataSource(String id, IDataSourceResolver resolver, JsonObject config) {
        Objects.requireNonNull(resolver);

        this.id = id;
        this.digest = ClickHouseUtils.digest(config);

        this.customColumns = new ArrayList<ClickHouseColumnInfo>();

        int cacheSize = 100;
        int cacheExpireMinute = 5;

        if (config == null) {
            this.timezone = null;
            this.defaultValues = new DefaultValues();
            this.queryParameters = new QueryParameters();
        } else {
            String tz = config.getString(CONF_TIMEZONE);
            this.timezone = tz == null ? null : TimeZone.getTimeZone(tz);

            JsonObject cacheConfig = config.getJsonObject(CONF_CACHE);
            if (cacheConfig != null) {
                for (Entry<String, Object> entry : cacheConfig) {
                    String cacheName = entry.getKey();
                    if (CONF_COLUMNS.equals(cacheName) && entry.getValue() instanceof JsonObject) {
                        JsonObject json = (JsonObject) entry.getValue();
                        cacheSize = json.getInteger(CONF_SIZE, cacheSize);
                        cacheExpireMinute = json.getInteger(CONF_EXPIRATION, cacheExpireMinute);
                        break;
                    }
                }
            }
            JsonArray array = config.getJsonArray(CONF_COLUMNS);
            if (array != null) {
                for (Object obj : array) {
                    if (obj instanceof JsonObject) {
                        this.customColumns.add(ClickHouseColumnInfo.fromJson((JsonObject) obj));
                    }
                }
            }
            this.defaultValues = new DefaultValues(config.getJsonObject(CONF_DEFAULTS));
            this.queryParameters = new QueryParameters(config.getJsonObject(CONF_PARAMETERS));
        }

        this.columnsCache = Caffeine.newBuilder().maximumSize(cacheSize)
                .expireAfterAccess(cacheExpireMinute, TimeUnit.MINUTES).build();
    }

    public final String getId() {
        return this.id;
    }

    public final TimeZone getTimeZone() {
        return this.timezone;
    }

    public final ClickHouseColumnList getColumns(String schema, String query) {
        final ClickHouseColumnList columns; // = ClickHouseColumnList.DEFAULT_COLUMNS_INFO;

        final ClickHouseDataSource self = this;

        try {
            columns = columnsCache.get(query, k -> {
                return inferColumns(schema, self.loadSavedQueryAsNeeded(k));
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to retrieve columns definition", e);
        }

        return columns;
    }

    public final boolean isDifferentFrom(JsonObject newConfig) {
        String newDigest = ClickHouseUtils.digest(newConfig == null ? null : newConfig.encode());
        boolean isDifferent = this.digest == null || this.digest.length() == 0 || !this.digest.equals(newDigest);
        if (isDifferent) {
            log.info("Datasource configuration of [{}] is changed from [{}] to [{}]", this.id, digest, newDigest);
        } else {
            log.info("Datasource configuration of [{}] remains the same", this.id);
        }

        return isDifferent;
    }

    public final List<ClickHouseColumnInfo> getCustomColumns() {
        return Collections.unmodifiableList(this.customColumns);
    }

    public final DefaultValues getDefaultValues() {
        return this.defaultValues;
    }

    public final QueryParameters newQueryParameters(QueryParameters paramsToMerge) {
        return new QueryParameters().merge(this.queryParameters).merge(paramsToMerge);
    }

    public final void executeQuery(ClickHouseNamedQuery query, ClickHouseColumnList requestColumns,
            QueryParameters params, ClickHouseResponseWriter writer) {
        Objects.requireNonNull(query);
        Objects.requireNonNull(requestColumns);
        Objects.requireNonNull(params);

        List<ClickHouseColumnInfo> additionalColumns = new ArrayList<ClickHouseColumnInfo>();
        if (params.showDatasourceColumn()) {
            additionalColumns.add(new ClickHouseColumnInfo(ClickHouseColumnList.COLUMN_DATASOURCE,
                    ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE, null, this.getId()));
        }
        if (params.showCustomColumns()) {
            additionalColumns.addAll(this.getCustomColumns());
        }
        requestColumns.updateValues(additionalColumns);

        ClickHouseColumnList allColumns = query.getColumns();

        for (int i = additionalColumns.size(); i < requestColumns.size(); i++) {
            ClickHouseColumnInfo r = requestColumns.getColumn(i);
            for (int j = 0; j < allColumns.size(); j++) {
                if (r.getName().equals(allColumns.getColumn(j).getName())) {
                    r.setIndex(j);
                    break;
                }
            }
        }

        executeQuery(loadSavedQueryAsNeeded(query.getQuery()), requestColumns, params, writer);
    }

    public final String loadSavedQueryAsNeeded(String normalizedQuery) {
        // in case the "normalizedQuery" is a local file...
        if (normalizedQuery.indexOf('\n') == -1 && isSavedQuery(normalizedQuery)
                && ClickHouseUtils.fileExists(normalizedQuery)) {
            normalizedQuery = ClickHouseUtils.loadTextFromFile(normalizedQuery);
        }

        return normalizedQuery;
    }

    @Override
    public void close() throws IOException {
        log.info("Closing datasource[id={}, instance={}]", this.id, this);
    }

    public void executeQuery(String query, ClickHouseColumnList columns, QueryParameters parameters,
            ClickHouseResponseWriter writer) {
        log.info("Executing query:\n{}", query);

        writeDebugInfo(this.id, getType(), null, query, parameters, writer);
    }

    public void executeUpdate(String schema, String table, ClickHouseColumnList columns, QueryParameters parameters,
            ClickHouseBuffer buffer) {
        log.info("Discard mutation: schema=[{}], table=[{}]", schema, table);
    }

    public String getQuoteIdentifier() {
        return DEFAULT_QUOTE_IDENTIFIER;
    }

    public String getType() {
        return DATASOURCE_TYPE;
    }
}