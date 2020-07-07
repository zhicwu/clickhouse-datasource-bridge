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
package com.github.clickhouse.bridge.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import com.github.clickhouse.bridge.core.ClickHouseResponseWriter;
import com.github.clickhouse.bridge.core.IDataSourceResolver;
import com.github.clickhouse.bridge.core.ClickHouseBuffer;
import com.github.clickhouse.bridge.core.ClickHouseColumnInfo;
import com.github.clickhouse.bridge.core.ClickHouseColumnList;
import com.github.clickhouse.bridge.core.ClickHouseDataSource;
import com.github.clickhouse.bridge.core.ClickHouseDataType;
import com.github.clickhouse.bridge.core.QueryParameters;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ClickHouseJdbcDataSource extends ClickHouseDataSource {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClickHouseJdbcDataSource.class);

    private static final Set<String> PRIVATE_PROPS = Collections
            .unmodifiableSet(new HashSet<>(Arrays.asList(CONF_SCHEMA, CONF_TYPE, CONF_TIMEZONE, CONF_CACHE)));

    private static final Properties DEFAULT_DATASOURCE_PROPERTIES = new Properties();

    private static final String PROP_POOL_NAME = "poolName";
    private static final String PROP_PASSWORD = "password";

    private static final String PROP_CLIENT_NAME = "ClientUser";
    private static final String DEFAULT_CLIENT_NAME = "clickhouse-datasource-bridge";

    private static final String QUERY_TABLE_BEGIN = "SELECT * FROM ";
    private static final String QUERY_TABLE_END = " WHERE 1 = 0";

    private static final String CONF_DATASOURCE = "dataSource";

    private static final String QUERY_FILE_EXT = ".sql";

    public static final String DATASOURCE_TYPE = "jdbc";

    static {
        // set default properties
        DEFAULT_DATASOURCE_PROPERTIES.setProperty("connectionTestQuery", "SELECT 1");
        DEFAULT_DATASOURCE_PROPERTIES.setProperty("minimumIdle", "1");
        DEFAULT_DATASOURCE_PROPERTIES.setProperty("maximumPoolSize", "5");
    }

    private final String jdbcUrl;
    private final HikariDataSource datasource;

    // cached identifier quote
    private String quoteIdentifier = null;

    public ClickHouseJdbcDataSource(String id, IDataSourceResolver resolver, JsonObject config) {
        super(id, resolver, config);

        Properties props = new Properties();
        props.putAll(DEFAULT_DATASOURCE_PROPERTIES);

        if (id != null && id.startsWith(DATASOURCE_TYPE) && config == null) { // adhoc
            this.jdbcUrl = id;
            this.datasource = null;
        } else { // named
            if (config != null) {
                for (Entry<String, Object> field : config) {
                    String key = field.getKey();

                    if (!PRIVATE_PROPS.contains(key)) {
                        Object value = field.getValue();

                        if (value instanceof JsonObject) {
                            if (CONF_DATASOURCE.equals(key)) {
                                for (Entry<String, Object> entry : (JsonObject) value) {
                                    String propName = entry.getKey();
                                    String propValue = String.valueOf(entry.getValue());
                                    if (!PROP_PASSWORD.equals(propName)) {
                                        propValue = resolver.resolve(propValue);
                                    }

                                    props.setProperty(
                                            new StringBuilder().append(key).append('.').append(propName).toString(),
                                            propValue);
                                }
                            }
                        } else if (value != null && !(value instanceof JsonArray)) {
                            String propValue = String.valueOf(value);
                            if (!PROP_PASSWORD.equals(key)) {
                                propValue = resolver.resolve(propValue);
                            }
                            props.setProperty(key, propValue);
                        }
                    }
                }
            }

            props.setProperty(PROP_POOL_NAME, id);

            this.jdbcUrl = null;
            this.datasource = new HikariDataSource(new HikariConfig(props));
        }
    }

    protected final Connection getConnection() throws SQLException {
        Connection conn = this.datasource != null ? this.datasource.getConnection()
                : DriverManager.getConnection(this.jdbcUrl);

        try {
            conn.setAutoCommit(true);
        } catch (Exception e) {
            log.warn("Failed enable auto commit due to {}", e.getMessage());
        }

        try {
            conn.setClientInfo(PROP_CLIENT_NAME, DEFAULT_CLIENT_NAME);
        } catch (Exception e) {
            log.warn("Failed call setClientInfo due to {}", e.getMessage());
        }

        return conn;
    }

    protected final Statement createStatement(Connection conn) throws SQLException {
        return createStatement(conn, null);
    }

    protected final Statement createStatement(Connection conn, QueryParameters parameters) throws SQLException {
        final Statement stmt;

        if (parameters == null) {
            stmt = conn.createStatement();
        } else {
            boolean scrollable = parameters.getPosition() != 0;
            stmt = conn.createStatement(scrollable ? ResultSet.TYPE_SCROLL_INSENSITIVE : ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);

            stmt.setFetchSize(parameters.getFetchSize());
            stmt.setMaxRows(parameters.getMaxRows());
        }

        return stmt;
    }

    protected final void skipRows(ResultSet rs, QueryParameters parameters) throws SQLException {
        if (rs != null && parameters != null) {
            int position = parameters.getPosition();
            // absolute position takes priority
            if (position != 0) {
                log.trace("Move cursor position to row #{}...", position);
                rs.absolute(position);
                log.trace("Now resume reading...");
            } else {
                int offset = parameters.getOffset();

                if (offset > 0) {
                    log.trace("Skipping first {} rows...", offset);
                    while (rs.next()) {
                        if (--offset <= 0) {
                            break;
                        }
                    }
                    log.trace("Now resume reading the rest rows...");
                }
            }
        }
    }

    protected final ClickHouseDataType convert(int jdbcType, boolean signed) {
        return convert(JDBCType.valueOf(jdbcType), signed);
    }

    protected ClickHouseDataType convert(JDBCType jdbcType, boolean signed) {
        ClickHouseDataType type = ClickHouseDataType.String;

        switch (jdbcType) {
            case BIT:
            case BOOLEAN:
                type = ClickHouseDataType.UInt8;
                break;
            case TINYINT:
                type = signed ? ClickHouseDataType.Int8 : ClickHouseDataType.UInt8;
                break;
            case SMALLINT:
                type = signed ? ClickHouseDataType.Int16 : ClickHouseDataType.UInt16;
                break;
            case INTEGER:
                type = signed ? ClickHouseDataType.Int32 : ClickHouseDataType.UInt32;
                break;
            case BIGINT:
                type = signed ? ClickHouseDataType.Int64 : ClickHouseDataType.UInt64;
                break;
            case REAL:
            case FLOAT:
                type = ClickHouseDataType.Float32;
                break;
            case DOUBLE:
                type = ClickHouseDataType.Float64;
                break;
            case NUMERIC:
            case DECIMAL:
                type = ClickHouseDataType.Decimal;
                break;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NVARCHAR:
            case LONGVARCHAR:
            case LONGNVARCHAR:
            case NULL:
                type = ClickHouseDataType.String;
                break;
            case DATE:
                type = ClickHouseDataType.Date;
                break;
            case TIME:
            case TIMESTAMP:
            case TIME_WITH_TIMEZONE:
            case TIMESTAMP_WITH_TIMEZONE:
                type = ClickHouseDataType.DateTime64;
                break;
            default:
                log.warn("Unsupported JDBC type [{}], which will be treated as [{}]", jdbcType.name(), type.name());
                break;
        }

        return type;
    }

    protected ResultSet getFirstQueryResult(Statement stmt, boolean hasResultSet) throws SQLException {
        ResultSet rs = null;

        if (hasResultSet) {
            rs = stmt.getResultSet();
        } else if (stmt.getUpdateCount() == -1) {
            throw new SQLException("No query result!");
        }

        return rs != null ? rs : getFirstQueryResult(stmt, stmt.getMoreResults());
    }

    @Override
    protected boolean isSavedQuery(String file) {
        return super.isSavedQuery(file) || file.endsWith(QUERY_FILE_EXT);
    }

    @Override
    protected ClickHouseColumnList inferColumns(String schema, String query) {
        log.debug("Inferring database columns: schema=[{}], query=[{}]", schema, query);

        try (Connection conn = getConnection(); Statement stmt = createStatement(conn)) {
            stmt.setMaxRows(1);
            stmt.setFetchSize(1);

            // could be just a table name
            if (query != null && query.indexOf(' ') == -1) {
                StringBuilder sb = new StringBuilder().append(QUERY_TABLE_BEGIN);
                String quote = this.getQuoteIdentifier();
                if (schema != null && schema.length() > 0) {
                    sb.append(quote).append(schema).append(quote).append('.');
                }
                query = sb.append(quote).append(query).append(quote).append(QUERY_TABLE_END).toString();
            }

            // could be very slow...
            ResultSetMetaData meta = getFirstQueryResult(stmt, stmt.execute(query)).getMetaData();

            ClickHouseColumnInfo[] columns = new ClickHouseColumnInfo[meta.getColumnCount()];

            for (int i = 1; i <= columns.length; i++) {
                boolean isSigned = true;
                int nullable = ResultSetMetaData.columnNullable;
                int precison = 0;
                int scale = 0;

                // Why try-catch? Try a not-fully implemented JDBC driver and you'll see...
                try {
                    isSigned = meta.isSigned(i);
                } catch (Exception e) {
                }

                try {
                    nullable = meta.isNullable(i);
                } catch (Exception e) {
                }

                try {
                    precison = meta.getPrecision(i);
                } catch (Exception e) {
                }

                try {
                    scale = meta.getScale(i);
                } catch (Exception e) {
                }

                columns[i - 1] = new ClickHouseColumnInfo(meta.getColumnName(i),
                        convert(meta.getColumnType(i), isSigned), ResultSetMetaData.columnNoNulls != nullable, precison,
                        scale);
            }

            return new ClickHouseColumnList(columns);
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to get columns definition from database", e);
        } catch (RuntimeException e) {
            throw e;
        }

        // return super.inferColumns(schema, query);
    }

    protected final void stream(ResultSet rs, ClickHouseColumnInfo[] columns, QueryParameters params,
            ClickHouseResponseWriter writer) throws SQLException {
        Objects.requireNonNull(rs);
        Objects.requireNonNull(columns);
        Objects.requireNonNull(writer);

        int length = columns.length;
        int estimatedSize = length * 4;
        int indexOffset = 0;
        int xLength = 0;
        if (params.showDatasourceColumn()) {
            xLength++;
        }
        if (params.showCustomColumns()) {
            xLength += this.getCustomColumns().size();
        }

        length -= xLength;
        estimatedSize += xLength * 4;
        indexOffset += xLength;

        while (rs.next()) {
            ClickHouseBuffer buffer = ClickHouseBuffer.newInstance(estimatedSize, this.getTimeZone());
            if (params.showDatasourceColumn()) {
                buffer.writeNonNull().writeString(this.getId());
            }
            if (params.showCustomColumns()) {
                for (int i = params.showDatasourceColumn() ? 1 : 0; i < xLength; i++) {
                    columns[i].writeValueTo(buffer);
                }
            }

            for (int i = 1; i <= length; i++) {
                ClickHouseColumnInfo column = columns[i - 1 + indexOffset];
                // keep in mind that column index is zero-based
                int index = column.isIndexed() ? column.getIndex() + 1 : i;

                if (column.isNullable()) {
                    if (rs.getObject(index) == null || rs.wasNull()) {
                        if (params.nullAsDefault()) {
                            // column.writeValueTo(buffer);
                            buffer.writeNonNull().writeDefaultValue(column, this.getDefaultValues());
                        } else {
                            buffer.writeNull();
                        }
                        continue;
                    } else {
                        buffer.writeNonNull();
                    }
                }

                switch (column.getType()) {
                    case Int8:
                        buffer.writeInt8(rs.getInt(index));
                        break;
                    case Int16:
                        buffer.writeInt16(rs.getInt(index));
                        break;
                    case Int32:
                        buffer.writeInt32(rs.getInt(index));
                        break;
                    case Int64:
                        buffer.writeInt64(rs.getLong(index));
                        break;
                    case UInt8:
                        buffer.writeUInt8(rs.getInt(index));
                        break;
                    case UInt16:
                        buffer.writeUInt16(rs.getInt(index));
                        break;
                    case UInt32:
                        buffer.writeUInt32(rs.getLong(index));
                        break;
                    case UInt64:
                        buffer.writeUInt64(rs.getLong(index));
                        break;
                    case Float32:
                        buffer.writeFloat32(rs.getFloat(index));
                        break;
                    case Float64:
                        buffer.writeFloat64(rs.getDouble(index));
                        break;
                    case Date:
                        buffer.writeDate(rs.getDate(index));
                        break;
                    case DateTime:
                        buffer.writeDateTime(rs.getTimestamp(index), column.getTimeZone());
                        break;
                    case DateTime64:
                        buffer.writeDateTime64(rs.getTimestamp(index), column.getTimeZone());
                        break;
                    case Decimal:
                        buffer.writeDecimal(rs.getBigDecimal(index), column.getPrecision(), column.getScale());
                        break;
                    case Decimal32:
                        buffer.writeDecimal32(rs.getBigDecimal(index), column.getScale());
                        break;
                    case Decimal64:
                        buffer.writeDecimal64(rs.getBigDecimal(index), column.getScale());
                        break;
                    case Decimal128:
                        buffer.writeDecimal128(rs.getBigDecimal(index), column.getScale());
                        break;
                    case String:
                    default:
                        buffer.writeString(rs.getString(index), params.nullAsDefault());
                        break;
                }
            }

            writer.write(buffer);
        }
    }

    @Override
    public final String getType() {
        return DATASOURCE_TYPE;
    }

    @Override
    public final String getQuoteIdentifier() {
        if (this.quoteIdentifier == null) {
            this.quoteIdentifier = DEFAULT_QUOTE_IDENTIFIER;

            try (Connection conn = getConnection()) {
                this.quoteIdentifier = conn.getMetaData().getIdentifierQuoteString().trim();
            } catch (Exception e) {
                log.warn("Failed to get identifier quote string", e);
            }
        }

        if (this.quoteIdentifier.isEmpty()) {
            log.warn("Identifier quote string cannot be empty string, had to change it to default");
            this.quoteIdentifier = DEFAULT_QUOTE_IDENTIFIER;
        }

        return this.quoteIdentifier;
    }

    @Override
    public void executeQuery(String query, ClickHouseColumnList columns, QueryParameters params,
            ClickHouseResponseWriter writer) {
        log.info("Executing SQL:\n{}", query);

        // String queryId = params.dedupQuery() ? this.generateUniqueQueryId(query) :
        // null;

        try (Connection conn = getConnection(); Statement stmt = createStatement(conn, params)) {
            stream(getFirstQueryResult(stmt, stmt.execute(query)), columns.getColumns(), params, writer);
            /*
             * if (stmt.execute(query)) { // TODO multiple resultsets
             * 
             * } else if (columns.size() == 1 && columns.getColumn(0).getType() ==
             * ClickHouseDataType.Int32) {
             * writer.write(ClickHouseBuffer.newInstance(4).writeInt32(stmt.getUpdateCount()
             * )); } else { throw new IllegalStateException(
             * "Not able to handle query result due to incompatible columns: " + columns); }
             */
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to execute SQL", e);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();

        if (this.datasource != null) {
            this.datasource.close();
        }
    }
}