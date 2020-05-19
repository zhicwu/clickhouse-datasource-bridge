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

import io.vertx.core.json.JsonObject;

public class QueryParameters {
    public static final String PARAM_FETCH_SIZE = "fetch_size";
    public static final String PARAM_MAX_ROWS = "max_rows";
    public static final String PARAM_NULL_AS_DEFAULT = "null_as_default";
    public static final String PARAM_OFFSET = "offset";
    public static final String PARAM_POSITION = "position";
    public static final String PARAM_DEBUG = "debug";

    public static final int DEFAULT_FETCH_SIZE = 1000;
    public static final int DEFAULT_MAX_ROWS = 0;
    public static final boolean DEFAULT_NULL_AS_DEFAULT = false;
    public static final int DEFAULT_OFFSET = 0;
    public static final int DEFAULT_POSITION = 0;
    public static final boolean DEFAULT_DEBUG = false;

    private int fetchSize = DEFAULT_FETCH_SIZE;
    private int maxRows = DEFAULT_MAX_ROWS;
    private boolean nullAsDefault = DEFAULT_NULL_AS_DEFAULT;
    private int offset = DEFAULT_OFFSET;
    private int position = DEFAULT_POSITION;
    private boolean debug = DEFAULT_DEBUG;

    public QueryParameters(String uri) {
        merge(uri);
    }

    public QueryParameters(JsonObject... params) {
        for (JsonObject parameters : params) {
            merge(parameters);
        }
    }

    public QueryParameters merge(QueryParameters params) {
        if (params != null) {
            fetchSize = params.fetchSize;
            maxRows = params.maxRows;
            nullAsDefault = params.nullAsDefault;
            offset = params.offset;
            position = params.position;
            debug = params.debug;
        }

        return this;
    }

    public QueryParameters merge(JsonObject parameters) {
        if (parameters != null) {
            fetchSize = parameters.getInteger(PARAM_FETCH_SIZE, DEFAULT_FETCH_SIZE);
            maxRows = parameters.getInteger(PARAM_MAX_ROWS, DEFAULT_MAX_ROWS);
            nullAsDefault = parameters.getBoolean(PARAM_NULL_AS_DEFAULT, DEFAULT_NULL_AS_DEFAULT);
            offset = parameters.getInteger(PARAM_OFFSET, DEFAULT_OFFSET);
            position = parameters.getInteger(PARAM_POSITION, DEFAULT_POSITION);
            debug = parameters.getBoolean(PARAM_DEBUG, DEFAULT_DEBUG);
        }

        return this;
    }

    public QueryParameters merge(String uri) {
        JsonObject params = new JsonObject();

        int index = uri == null ? -1 : uri.indexOf('?');
        if (index >= 0 && uri.length() > index) {
            String query = uri.substring(index + 1);

            for (String param : ClickHouseBuffer.splitByChar(query, '&')) {
                index = param.indexOf('=');
                if (index > 0) {
                    String key = param.substring(0, index);
                    String value = param.substring(index + 1);

                    if (PARAM_FETCH_SIZE.equals(key) || PARAM_MAX_ROWS.equals(key) || PARAM_OFFSET.equals(key)
                            || PARAM_POSITION.equals(key)) {
                        params.put(key, Integer.valueOf(value));
                    } else if (PARAM_NULL_AS_DEFAULT.equals(key) || PARAM_DEBUG.equals(key)) {
                        params.put(key, Boolean.valueOf(value));
                    } else {
                        params.put(key, value);
                    }
                }
            }
        }

        return merge(params);
    }

    public int getFetchSize() {
        return this.fetchSize;
    }

    public int getMaxRows() {
        return this.maxRows;
    }

    public boolean replaceNullAsDefault() {
        return this.nullAsDefault;
    }

    public int getOffset() {
        return this.offset;
    }

    public int getPosition() {
        return this.position;
    }

    public boolean isDebug() {
        return debug;
    }

    public String toQueryString() {
        return new StringBuilder().append(PARAM_FETCH_SIZE).append('=').append(fetchSize).append('&')
                .append(PARAM_MAX_ROWS).append('=').append(maxRows).append('&').append(PARAM_OFFSET).append('=')
                .append(offset).append('&').append(PARAM_POSITION).append('=').append(position).append('&')
                .append(PARAM_NULL_AS_DEFAULT).append('=').append(nullAsDefault).toString();
    }
}