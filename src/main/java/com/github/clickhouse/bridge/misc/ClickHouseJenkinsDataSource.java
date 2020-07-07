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
package com.github.clickhouse.bridge.misc;

import com.github.clickhouse.bridge.core.ClickHouseColumnInfo;
import com.github.clickhouse.bridge.core.ClickHouseColumnList;
import com.github.clickhouse.bridge.core.ClickHouseDataSource;
import com.github.clickhouse.bridge.core.ClickHouseDataType;
import com.github.clickhouse.bridge.core.ClickHouseResponseWriter;
import com.github.clickhouse.bridge.core.IDataSourceResolver;
import com.github.clickhouse.bridge.core.QueryParameters;

import io.vertx.core.json.JsonObject;

import static com.github.clickhouse.bridge.core.ClickHouseDataType.*;

public class ClickHouseJenkinsDataSource extends ClickHouseDataSource {
    public static final String DATASOURCE_TYPE = "jenkins";

    private static final ClickHouseColumnList JENKINS_JOBS_COLUMNS = new ClickHouseColumnList(
            new ClickHouseColumnInfo("class", ClickHouseDataType.String, false, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ClickHouseColumnInfo("name", ClickHouseDataType.String, false, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ClickHouseColumnInfo("url", ClickHouseDataType.String, false, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ClickHouseColumnInfo("color", ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE));

    public ClickHouseJenkinsDataSource(String id, IDataSourceResolver resolver, JsonObject config) {
        super(id, resolver, config);
    }

    @Override
    public void executeQuery(String query, ClickHouseColumnList columns, QueryParameters parameters,
            ClickHouseResponseWriter writer) {
        super.executeQuery(query, columns, parameters, writer);
    }

    @Override
    public String getType() {
        return DATASOURCE_TYPE;
    }

    @Override
    protected ClickHouseColumnList inferColumns(String schema, String query) {
        return JENKINS_JOBS_COLUMNS;
    }

}