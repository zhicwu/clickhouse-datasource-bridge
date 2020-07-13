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
import java.util.TimeZone;

import io.vertx.core.json.JsonObject;

import static com.github.clickhouse.bridge.core.ClickHouseDataType.*;

public class ClickHouseColumnInfo {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClickHouseColumnInfo.class);

    public static final String DEFAULT_NAME = "unknown";
    public static final ClickHouseDataType DEFAULT_TYPE = ClickHouseDataType.String;

    private static final boolean DEFAULT_VALUE_SUPPORT = false;

    private static final String CONF_NAME = "name";
    private static final String CONF_TYPE = "type";
    private static final String CONF_VALUE = "value";
    private static final String CONF_NULLABLE = "nullable";
    private static final String CONF_PRECISION = "precision";
    private static final String CONF_SCALE = "scale";
    private static final String CONF_TIMEZONE = "timezone";

    private static final String TOKEN_DEFAULT = "DEFAULT";
    private static final String NULLABLE_BEGIN = "Nullable(";
    private static final String NULLABLE_END = ")";

    private final String name;
    private final ClickHouseDataType type;
    private final boolean nullable;
    private final int precision;
    private final int scale;
    private final TimeZone timezone;
    private final boolean hasDefaultValue;

    final TypedParameter<?> value;

    // index in the column list
    private int index = -1;

    public static ClickHouseColumnInfo fromJson(JsonObject json) {
        String name = DEFAULT_NAME;
        ClickHouseDataType type = ClickHouseDataType.String;
        boolean nullable = DEFAULT_NULLABLE;
        int precision = DEFAULT_PRECISION;
        int scale = DEFAULT_SCALE;
        String timezone = null;
        String value = null;

        if (json != null) {
            name = json.getString(CONF_NAME, DEFAULT_NAME);
            type = ClickHouseDataType.valueOf(json.getString(CONF_TYPE, DEFAULT_TYPE.name()));
            nullable = json.getBoolean(CONF_NULLABLE, DEFAULT_NULLABLE);
            switch (type) {
                case DateTime64:
                    scale = json.getInteger(CONF_SCALE, DEFAULT_DATETIME64_SCALE);
                    break;
                case Decimal:
                    precision = json.getInteger(CONF_PRECISION, DEFAULT_PRECISION);
                case Decimal32:
                case Decimal64:
                case Decimal128:
                    scale = json.getInteger(CONF_SCALE, DEFAULT_SCALE);
                    break;
                default:
                    break;
            }

            timezone = json.getString(CONF_TIMEZONE);
            value = json.getString(CONF_VALUE);
        }

        return new ClickHouseColumnInfo(name, type, nullable, precision, scale, timezone, value);
    }

    public static ClickHouseColumnInfo fromString(String columnInfo) {
        String name = DEFAULT_NAME;
        ClickHouseDataType type = DEFAULT_TYPE;
        boolean nullable = DEFAULT_NULLABLE;
        int precision = -1;
        int scale = -1;
        String timezone = null;
        String value = null;

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
                    String defaultValue = null;
                    if (declaredType.startsWith(NULLABLE_BEGIN)) {
                        int suffixIndex = declaredType.lastIndexOf(NULLABLE_END);
                        if (suffixIndex != -1) {
                            nullable = true;
                            defaultValue = declaredType.substring(suffixIndex + NULLABLE_END.length()).trim();
                            declaredType = declaredType.substring(NULLABLE_BEGIN.length(), suffixIndex);

                            i = columnInfo.length() - 1;
                        } else {
                            log.warn("Discard invalid Nullable declaration [{}]", declaredType);
                        }
                    } else {
                        nullable = false;

                        if (DEFAULT_VALUE_SUPPORT) {
                            // FIXME this is buggy
                            int defaultIndex = declaredType.indexOf(' ');

                            if (defaultIndex != -1) {
                                defaultValue = declaredType.substring(defaultIndex + 1).trim();
                                declaredType = declaredType.substring(0, defaultIndex);
                            }
                        }
                        i = columnInfo.length() - 1;
                    }

                    // datetime, datetime64 and decimals
                    int index = declaredType.indexOf('(');
                    if (index > 0 && declaredType.charAt(declaredType.length() - 1) == ')') {
                        List<String> arguments = ClickHouseUtils
                                .splitByChar(declaredType.substring(index + 1, declaredType.length() - 1), ',');
                        type = ClickHouseDataType.valueOf(declaredType.substring(0, index));

                        int size = arguments.size();
                        if (size > 0) {
                            switch (type) {
                                case DateTime64:
                                    scale = Integer.parseInt(arguments.remove(0));
                                case DateTime:
                                    if (arguments.size() > 0) {
                                        String tz = arguments.remove(0).trim();
                                        if (tz.length() > 2 && tz.charAt(0) == '\''
                                                && tz.charAt(tz.length() - 1) == '\'') {
                                            timezone = tz.substring(1, tz.length() - 1);
                                        }
                                    }
                                    break;
                                case Decimal:
                                    precision = Integer.parseInt(arguments.remove(0));
                                case Decimal32:
                                case Decimal64:
                                case Decimal128:
                                    if (arguments.size() > 0) {
                                        scale = Integer.parseInt(arguments.remove(0));
                                    }
                                default:
                                    log.warn("Discard unsupported arguments for [{}]: {}", declaredType, arguments);
                                    break;
                            }

                            if (arguments.size() > 0) {
                                log.warn("Discard unsupported arguments for [{}]: {}", declaredType, arguments);
                            }
                        } else {
                            log.warn("Discard empty argument for [{}]", declaredType);
                        }
                    } else {
                        type = ClickHouseDataType.valueOf(declaredType);
                    }

                    // default value
                    if (DEFAULT_VALUE_SUPPORT && defaultValue != null && defaultValue.startsWith(TOKEN_DEFAULT)) {
                        defaultValue = defaultValue.substring(TOKEN_DEFAULT.length()).trim();
                        value = defaultValue.charAt(0) == '\'' && defaultValue.charAt(defaultValue.length() - 1) == '\''
                                ? defaultValue.substring(1, defaultValue.length() - 1)
                                : defaultValue;
                    }
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

        return new ClickHouseColumnInfo(name, type, nullable, precision, scale, timezone, value);
    }

    public ClickHouseColumnInfo(String name, ClickHouseDataType type, boolean nullable, int precision, int scale) {
        this(name, type, nullable, precision, scale, null, null);
    }

    public ClickHouseColumnInfo(String name, ClickHouseDataType type, boolean nullable, int precision, int scale,
            String timezone, String value) {
        this.name = name == null ? DEFAULT_NAME : name;
        this.type = type;
        this.nullable = nullable;
        this.timezone = type == ClickHouseDataType.DateTime || type == ClickHouseDataType.DateTime64
                ? (timezone == null ? null : TimeZone.getTimeZone(timezone))
                : null;
        this.hasDefaultValue = value != null;
        this.value = new DefaultValues().getTypedValue(type).merge(value);

        int recommendedScale = DEFAULT_SCALE;

        switch (type) {
            case Decimal:
                int recommendedPrecision = DEFAULT_DECIMAL_PRECISON;
                this.precision = precision <= 0 ? recommendedPrecision
                        : (precision > MAX_PRECISON ? MAX_PRECISON : precision);
                recommendedScale = DEFAULT_DECIMAL_SCALE;
                break;
            case Decimal32:
                this.precision = DEFAULT_DECIMAL32_PRECISON;
                recommendedScale = DEFAULT_DECIMAL32_SCALE;
                break;
            case Decimal64:
                this.precision = DEFAULT_DECIMAL64_PRECISON;
                recommendedScale = DEFAULT_DECIMAL64_SCALE;
                break;
            case Decimal128:
                this.precision = DEFAULT_DECIMAL128_PRECISON;
                recommendedScale = DEFAULT_DECIMAL128_SCALE;
                break;
            default:
                this.precision = precision < 0 ? DEFAULT_PRECISION : precision;
                break;
        }

        this.scale = this.type == ClickHouseDataType.DateTime64 ? DEFAULT_DATETIME64_SCALE
                : (scale <= 0 ? recommendedScale : (scale > this.precision ? this.precision : scale));
    }

    public String getName() {
        return this.name;
    }

    public ClickHouseDataType getType() {
        return this.type;
    }

    public Object getValue() {
        return this.value.getValue();
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

    public TimeZone getTimeZone() {
        return this.timezone;
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

    public void writeValueTo(ClickHouseBuffer buffer) {
        if (buffer == null) {
            return;
        }

        if (this.isNullable()) {
            buffer.writeNonNull();
        }

        this.value.writeValueTo(buffer, this.getPrecision(), this.getScale(), this.getTimeZone());
    }

    JsonObject toJson() {
        JsonObject col = new JsonObject();
        col.put(CONF_NAME, this.getName());
        col.put(CONF_TYPE, this.getType().name());
        col.put(CONF_NULLABLE, this.isNullable());

        switch (this.getType()) {
            case DateTime:
                if (this.getTimeZone() != null) {
                    col.put(CONF_TIMEZONE, this.getTimeZone().getID());
                }
                break;
            case Decimal:
                col.put(CONF_PRECISION, this.getPrecision());
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case DateTime64:
                col.put(CONF_SCALE, this.getScale());
                break;
            default:
                break;
        }

        if (this.hasDefaultValue) {
            col.put(CONF_VALUE, this.getValue());
        }

        return col;
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
        } else if (this.type == ClickHouseDataType.DateTime && this.timezone != null) {
            sb.append('(').append('\'').append(this.timezone.getID()).append('\'').append(')');
        } else if (this.type == ClickHouseDataType.DateTime64) {
            sb.append('(').append(this.scale);
            if (this.timezone != null) {
                sb.append(',').append('\'').append(this.timezone.getID()).append('\'');
            }
            sb.append(')');
        }

        if (this.nullable) {
            sb.insert(index, NULLABLE_BEGIN).append(NULLABLE_END);
        }

        if (DEFAULT_VALUE_SUPPORT && this.hasDefaultValue) {
            sb.append(' ').append(TOKEN_DEFAULT).append(' ');
            if (this.type == ClickHouseDataType.String) {
                sb.append('\'').append(this.getValue()).append('\'');
            } else {
                sb.append(this.getValue());
            }
        }

        return sb.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (hasDefaultValue ? 1231 : 1237);
        result = prime * result + index;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + (nullable ? 1231 : 1237);
        result = prime * result + precision;
        result = prime * result + scale;
        result = prime * result + ((timezone == null) ? 0 : timezone.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;

        ClickHouseColumnInfo other = (ClickHouseColumnInfo) obj;

        return (index == other.index && nullable == other.nullable && precision == other.precision
                && hasDefaultValue == other.hasDefaultValue && scale == other.scale && type == other.type
                && (name == other.name || (name != null && name.equals(other.name)))
                && (timezone == other.timezone || (timezone != null && timezone.equals(other.timezone)))
                && (value == other.value || (value != null && value.equals(other.value))));
    }
}