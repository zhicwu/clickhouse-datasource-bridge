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

import java.util.Arrays;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ClickHouseColumnList {
    public static final int DEFAULT_VERSION = 1;

    public static final ClickHouseColumnList DEFAULT_COLUMNS_INFO = new ClickHouseColumnList(
            new ClickHouseColumnInfo("datasource", ClickHouseDataType.String, true,
                    ClickHouseColumnInfo.DEFAULT_PRECISION, ClickHouseColumnInfo.DEFAULT_SCALE),
            new ClickHouseColumnInfo("type", ClickHouseDataType.String, true, ClickHouseColumnInfo.DEFAULT_PRECISION,
                    ClickHouseColumnInfo.DEFAULT_SCALE),
            new ClickHouseColumnInfo("query", ClickHouseDataType.String, true, ClickHouseColumnInfo.DEFAULT_PRECISION,
                    ClickHouseColumnInfo.DEFAULT_SCALE),
            new ClickHouseColumnInfo("parameters", ClickHouseDataType.String, true,
                    ClickHouseColumnInfo.DEFAULT_PRECISION, ClickHouseColumnInfo.DEFAULT_SCALE));

    private static final String COLUMN_HEADER = "columns format version: ";
    private static final String COLUMN_COUNT = " columns:";

    private static final String CONF_VERSION = "version";
    private static final String CONF_LIST = "list";

    private final int version;
    private final ClickHouseColumnInfo[] columns;

    public ClickHouseColumnList(ClickHouseColumnInfo... columns) {
        this(1, columns);
    }

    public ClickHouseColumnList(int version, ClickHouseColumnInfo... columns) {
        this.version = version;
        this.columns = new ClickHouseColumnInfo[columns.length];

        for (int i = 0; i < columns.length; i++) {
            ClickHouseColumnInfo column = columns[i];
            this.columns[i] = new ClickHouseColumnInfo(
                    column.getName() == ClickHouseColumnInfo.DEFAULT_NAME ? String.valueOf(i + 1) : column.getName(),
                    column.getType(), column.isNullable(), column.getPrecision(), column.getScale());
        }
    }

    public static ClickHouseColumnList fromJson(JsonObject config) {
        int version = DEFAULT_VERSION;
        ClickHouseColumnInfo[] columns = new ClickHouseColumnInfo[0];

        if (config != null) {
            version = config.getInteger(CONF_VERSION, DEFAULT_VERSION);
            JsonArray array = config.getJsonArray(CONF_LIST);

            if (array != null) {
                columns = new ClickHouseColumnInfo[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    columns[i] = ClickHouseColumnInfo.fromJson(array.getJsonObject(i));
                }
            }
        }

        return new ClickHouseColumnList(version, columns);
    }

    public static ClickHouseColumnList fromString(String columnsInfo) {
        int version = DEFAULT_VERSION;
        ClickHouseColumnInfo[] columns = new ClickHouseColumnInfo[0];

        if (columnsInfo != null && columnsInfo.startsWith(COLUMN_HEADER)) {
            List<String> lines = ClickHouseBuffer.splitByChar(columnsInfo, '\n');
            columns = new ClickHouseColumnInfo[lines.size() - 2];

            int index = 0;

            String currentLine = null;
            try {
                for (String c : lines) {
                    currentLine = c;

                    if (index == 0) {
                        version = Integer.parseInt(c.substring(COLUMN_HEADER.length()));
                    } else if (index == 1) {
                        if (!c.endsWith(COLUMN_COUNT)) {
                            throw new IllegalArgumentException(new StringBuilder().append("line #").append(index + 1)
                                    .append(" must be end with '").append(COLUMN_COUNT).append('\'').toString());
                        }

                        String cCount = c.substring(0, c.length() - COLUMN_COUNT.length());
                        if (columns.length < Integer.parseInt(cCount)) {
                            throw new IllegalArgumentException(
                                    new StringBuilder().append("inconsistent columns count: declared ").append(cCount)
                                            .append(" but looks like ").append(lines.size()).toString());
                        }
                    } else {
                        columns[index - 2] = ClickHouseColumnInfo.fromString(c);
                    }

                    index++;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(new StringBuilder().append("failed to parse line #")
                        .append(index + 1).append(":\n").append(currentLine).toString(), e);
            }
        }

        return new ClickHouseColumnList(version, columns);
    }

    public int getVersion() {
        return this.version;
    }

    public boolean hasColumn() {
        return this.columns.length > 0;
    }

    public int size() {
        return this.columns.length;
    }

    public ClickHouseColumnInfo getColumn(int index) {
        return this.columns[index];
    }

    public ClickHouseColumnInfo[] getColumns() {
        return Arrays.copyOf(this.columns, this.columns.length);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(COLUMN_HEADER).append(version).append('\n').append(columns.length).append(COLUMN_COUNT).append('\n');

        for (ClickHouseColumnInfo column : columns) {
            sb.append(column.toString()).append('\n');
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(columns);
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ClickHouseColumnList other = (ClickHouseColumnList) obj;

        return version == other.version && Arrays.equals(columns, other.columns);
    }
}