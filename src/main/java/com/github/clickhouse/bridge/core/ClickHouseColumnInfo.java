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

import java.util.List;

import io.vertx.core.json.JsonObject;

public class ClickHouseColumnInfo {
    public static final String DEFAULT_NAME = "unknown";
    public static final ClickHouseDataType DEFAULT_TYPE = ClickHouseDataType.String;
    public static final boolean DEFAULT_NULLABLE = true;
    public static final int DEFAULT_PRECISION = 0;
    public static final int DEFAULT_SCALE = 0;

    private static final String CONF_NAME = "name";
    private static final String CONF_TYPE = "type";
    private static final String CONF_NULLABLE = "nullable";
    private static final String CONF_PRECISION = "precision";
    private static final String CONF_SCALE = "scale";

    private static final String NULLABLE_BEGIN = "Nullable(";
    private static final String NULLABLE_END = ")";

    private final String name;
    private final ClickHouseDataType type;
    private final boolean nullable;
    private final int precision;
    private final int scale;

    // index in the column list
    private int index = -1;

    public static ClickHouseColumnInfo fromJson(JsonObject json) {
        return json == null
                ? new ClickHouseColumnInfo(DEFAULT_NAME, ClickHouseDataType.String, DEFAULT_NULLABLE, DEFAULT_PRECISION,
                        DEFAULT_SCALE)
                : new ClickHouseColumnInfo(json.getString(CONF_NAME, DEFAULT_NAME),
                        ClickHouseDataType.valueOf(json.getString(CONF_TYPE, DEFAULT_TYPE.name())),
                        json.getBoolean(CONF_NULLABLE, DEFAULT_NULLABLE),
                        json.getInteger(CONF_PRECISION, DEFAULT_PRECISION), json.getInteger(CONF_SCALE, DEFAULT_SCALE));
    }

    public static ClickHouseColumnInfo fromString(String columnInfo) {
        String name = DEFAULT_NAME;
        ClickHouseDataType type = DEFAULT_TYPE;
        boolean nullable = DEFAULT_NULLABLE;
        int precision = -1;
        int scale = -1;

        if (columnInfo != null && (columnInfo = columnInfo.trim()).length() > 0) {
            char quote = columnInfo.charAt(0);
            boolean hasQuote = quote == '`' || quote == '"';
            boolean escaped = false;
            int lastIndex = columnInfo.length() - 1;
            int nameEndIndex = hasQuote ? Math.min(columnInfo.lastIndexOf(quote), lastIndex) : lastIndex;
            StringBuilder sb = new StringBuilder(lastIndex + 1);
            for (int i = hasQuote ? 1 : 0; i <= lastIndex; i++) {
                char ch = columnInfo.charAt(i);
                escaped = hasQuote && !escaped && ch == quote && columnInfo.charAt(Math.min(i + 1, lastIndex)) == quote;

                if ((hasQuote && !escaped && i == nameEndIndex) || (!hasQuote && Character.isWhitespace(ch))) {
                    name = sb.toString();
                    sb.setLength(0);

                    // type declaration is case-sensitive
                    String declaredType = columnInfo.substring(Math.min(i + 1, lastIndex)).trim();
                    if (declaredType.startsWith(NULLABLE_BEGIN) && declaredType.endsWith(NULLABLE_END)) {
                        nullable = true;
                        declaredType = declaredType.substring(NULLABLE_BEGIN.length(),
                                declaredType.length() - NULLABLE_END.length());
                    } else {
                        nullable = false;
                    }

                    // decimals
                    int index = declaredType.indexOf('(');
                    if (index > 0 && declaredType.charAt(declaredType.length() - 1) == ')') {
                        List<String> precisionAndScale = ClickHouseBuffer
                                .splitByChar(declaredType.substring(index + 1, declaredType.length() - 1), ',');
                        declaredType = declaredType.substring(0, index);
                        if (precisionAndScale.size() == 1) {
                            scale = Integer.parseInt(precisionAndScale.get(0));
                        } else if (precisionAndScale.size() == 2) {
                            precision = Integer.parseInt(precisionAndScale.get(0));
                            scale = Integer.parseInt(precisionAndScale.get(1));
                        }
                    }

                    type = ClickHouseDataType.valueOf(declaredType);
                } else if (name == DEFAULT_NAME) {
                    if (!hasQuote || (hasQuote && !escaped)) {
                        sb.append(ch);
                    }
                }
            }

            if (sb.length() > 0) {
                name = sb.toString();
            }
        }

        return new ClickHouseColumnInfo(name, type, nullable, precision, scale);
    }

    public ClickHouseColumnInfo(String name, ClickHouseDataType type, boolean nullable, int precision, int scale) {
        this.name = name == null ? DEFAULT_NAME : name;
        this.type = type;
        this.nullable = nullable;

        int recommendedScale = DEFAULT_SCALE;

        switch (type) {
        case Decimal:
            int recommendedPrecision = 10;
            this.precision = precision <= 0 ? recommendedPrecision : precision;
            recommendedScale = 4;
            break;
        case Decimal32:
            this.precision = 9;
            recommendedScale = 2;
            break;
        case Decimal64:
            this.precision = 18;
            recommendedScale = 4;
            break;
        case Decimal128:
            this.precision = 38;
            recommendedScale = 8;
            break;
        default:
            this.precision = precision < 0 ? DEFAULT_PRECISION : precision;
            break;
        }

        this.scale = scale <= 0 ? recommendedScale : (scale > this.precision ? this.precision : scale);
    }

    public String getName() {
        return this.name;
    }

    public ClickHouseDataType getType() {
        return this.type;
    }

    public boolean isNullable() {
        return this.nullable;
    }

    public int getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }

    public void setIndex(int index) {
        if (index < 0) {
            throw new IllegalArgumentException("Column index is zero-based and should never be negative.");
        }

        if (this.index == -1) {
            this.index = index;
        } else {
            throw new IllegalStateException("Column index can only be set once!");
        }
    }

    public int getIndex() {
        return this.index;
    }

    public boolean isIndexed() {
        return this.index != -1;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        char quote = '`';
        sb.append(quote);
        for (int i = 0; i < this.name.length(); i++) {
            char ch = this.name.charAt(i);
            if (ch == quote) {
                sb.append(quote).append(quote);
            } else {
                sb.append(ch);
            }
        }

        sb.append(quote).append(' ');

        int index = sb.length();

        sb.append(this.type.name());

        if (this.type == ClickHouseDataType.Decimal) {
            sb.append('(').append(this.precision).append(',').append(this.scale).append(')');
        } else if (this.type == ClickHouseDataType.Decimal32 || this.type == ClickHouseDataType.Decimal64
                || this.type == ClickHouseDataType.Decimal128) {
            sb.append('(').append(this.scale).append(')');
        }

        if (this.nullable) {
            sb.insert(index, NULLABLE_BEGIN).append(NULLABLE_END);
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + (nullable ? 1231 : 1237);
        result = prime * result + precision;
        result = prime * result + scale;
        result = prime * result + ((type == null) ? 0 : type.hashCode());
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

        ClickHouseColumnInfo other = (ClickHouseColumnInfo) obj;

        return type == other.type && nullable == other.nullable
                && ((name == null && name == other.name) || (name != null && name.equals(other.name)))
                && (precision == other.precision) && (scale == other.scale);
    }
}