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

import static com.github.clickhouse.bridge.core.ClickHouseDataType.*;

public class ClickHouseColumnList {
    public static final int DEFAULT_VERSION = 1;

    public static final String COLUMN_DATASOURCE = "datasource";

    public static final ClickHouseColumnList DEFAULT_COLUMNS_INFO = new ClickHouseColumnList(
            new ClickHouseColumnInfo(COLUMN_DATASOURCE, ClickHouseDataType.String, true, DEFAULT_PRECISION,
                    DEFAULT_SCALE),
            new ClickHouseColumnInfo("type", ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ClickHouseColumnInfo("definition", ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ClickHouseColumnInfo("query", ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE),
            new ClickHouseColumnInfo("parameters", ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE));

    private static final String COLUMN_HEADER = "columns format version: ";
    private static final String COLUMN_COUNT = " columns:";

    private static final String CONF_VERSION = "version";
    private static final String CONF_QUERY = "query";
    private static final String CONF_COLUMNS = "columns";

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
                    column.getName() == ClickHouseColumnInfo.DEFAULT_NAME ? Integer.toString(i + 1) : column.getName(),
                    column.getType(), column.isNullable(), column.getPrecision(), column.getScale());
        }
    }

    public ClickHouseColumnList(ClickHouseColumnList template, boolean insert, ClickHouseColumnInfo... columns) {
        this.version = template.version;
        this.columns = new ClickHouseColumnInfo[template.columns.length + columns.length];

        if (insert) {
            System.arraycopy(columns, 0, this.columns, 0, columns.length);
            System.arraycopy(template.columns, 0, this.columns, columns.length, template.columns.length);
        } else { // append
            System.arraycopy(template.columns, 0, this.columns, 0, template.columns.length);
            System.arraycopy(columns, 0, this.columns, template.columns.length, columns.length);
        }
    }

    public static ClickHouseColumnList fromJson(JsonArray config) {
        int version = DEFAULT_VERSION;
        ClickHouseColumnInfo[] columns = new ClickHouseColumnInfo[0];

        if (config == null) {
            columns = new ClickHouseColumnInfo[0];
        } else {
            columns = new ClickHouseColumnInfo[config.size()];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = ClickHouseColumnInfo.fromJson(config.getJsonObject(i));
            }
        }

        return new ClickHouseColumnList(version, columns);
    }

    public static ClickHouseColumnList fromString(String columnsInfo) {
        int version = DEFAULT_VERSION;
        ClickHouseColumnInfo[] columns = new ClickHouseColumnInfo[0];

        if (columnsInfo != null && columnsInfo.startsWith(COLUMN_HEADER)) {
            List<String> lines = ClickHouseUtils.splitByChar(columnsInfo, '\n');
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

    public boolean containsColumn(String columnName) {
        boolean found = false;

        for (ClickHouseColumnInfo col : this.columns) {
            if (col.getName().equals(columnName)) {
                found = true;
                break;
            }
        }

        return found;
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

    public void updateValues(List<ClickHouseColumnInfo> refColumns) {
        for (int i = 0, size = refColumns == null ? 0 : refColumns.size(); i < size; i++) {
            ClickHouseColumnInfo refCol = refColumns.get(i);
            if (i < this.size()) {
                ClickHouseColumnInfo col = this.getColumn(i);
                col.value.merge(refCol.value.getValue().toString());
            }
        }
    }

    public String toJsonString(String query) {
        JsonObject config = new JsonObject();
        config.put(CONF_VERSION, this.version);
        if (query != null) {
            config.put(CONF_QUERY, query);
        }

        if (this.columns != null && this.columns.length > 0) {
            JsonArray array = new JsonArray();
            for (ClickHouseColumnInfo info : this.columns) {
                array.add(info.toJson());
            }

            config.put(CONF_COLUMNS, array);
        }

        return config.toString();
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