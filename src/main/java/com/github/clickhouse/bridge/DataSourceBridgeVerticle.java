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
package com.github.clickhouse.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import com.github.clickhouse.bridge.core.ClickHouseBuffer;
import com.github.clickhouse.bridge.core.ClickHouseColumnInfo;
import com.github.clickhouse.bridge.core.ClickHouseColumnList;
import com.github.clickhouse.bridge.core.ClickHouseDataSource;
import com.github.clickhouse.bridge.core.ClickHouseDataType;
import com.github.clickhouse.bridge.core.ClickHouseNamedQuery;
import com.github.clickhouse.bridge.core.ClickHouseResponseWriter;
import com.github.clickhouse.bridge.core.ClickHouseUtils;
import com.github.clickhouse.bridge.core.QueryParameters;
import com.github.clickhouse.bridge.core.QueryParser;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.ext.web.handler.TimeoutHandler;

import static com.github.clickhouse.bridge.core.ClickHouseDataType.*;

/**
 * Unified data source bridge for ClickHouse.
 *
 * @author Zhichun Wu
 */
public class DataSourceBridgeVerticle extends AbstractVerticle {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DataSourceBridgeVerticle.class);

    private static long startTime;

    private static final String CONFIG_PATH = "config";

    private static final int DEFAULT_SERVER_PORT = 9019;

    private static final String RESPONSE_CONTENT_TYPE = "application/octet-stream";

    private static final String WRITE_RESPONSE = "Ok.";
    private static final String PING_RESPONSE = WRITE_RESPONSE + "\n";

    private final ClickHouseDataSourceManager datasources = new ClickHouseDataSourceManager();
    private final ClickHouseNamedQueryManager queries = new ClickHouseNamedQueryManager();

    @Override
    public void start() {
        JsonObject config = ClickHouseUtils.loadJsonFromFile(CONFIG_PATH + "/server.json");

        datasources.registerTypes(config.getJsonObject("datasources"));

        long scanPeriod = config.getLong("configScanPeriod", 5000L);

        initConfig(CONFIG_PATH + "/datasources", scanPeriod, datasources::reload);
        initConfig(CONFIG_PATH + "/queries", scanPeriod, queries::reload);

        startServer(config, ClickHouseUtils.loadJsonFromFile(CONFIG_PATH + "/httpd.json"));
    }

    private void initConfig(String configPath, long scanPeriod, Consumer<JsonObject> loader) {
        ConfigRetriever retriever = ConfigRetriever.create(vertx,
                new ConfigRetrieverOptions().setScanPeriod(scanPeriod)
                        .addStore(new ConfigStoreOptions().setType("directory")
                                .setConfig(new JsonObject().put("path", configPath).put("filesets", new JsonArray()
                                        .add(new JsonObject().put("pattern", "*.json").put("format", "json"))))));

        retriever.getConfig(action -> {
            if (action.succeeded()) {
                try {
                    loader.accept(action.result());
                } catch (Exception e) {
                    log.error("Failed to load configuration", e);
                }
            } else {
                log.warn("Not able to load configuration from [{}] due to {}", configPath, action.cause().getMessage());
            }
        });

        retriever.listen(change -> {
            log.info("Configuration change in [{}] detected", configPath);

            try {
                loader.accept(change.getNewConfiguration());
            } catch (Exception e) {
                log.error("Failed to reload configuration", e);
            }
        });
    }

    private void startServer(JsonObject bridgeServerConfig, JsonObject httpServerConfig) {
        HttpServer server = vertx.createHttpServer(new HttpServerOptions(httpServerConfig));
        // vertx.createHttpServer(new
        // HttpServerOptions().setTcpNoDelay(false).setTcpKeepAlive(true)
        // .setTcpFastOpen(true).setLogActivity(true));

        // https://github.com/vert-x3/vertx-examples/blob/master/web-examples/src/main/java/io/vertx/example/web/mongo/Server.java
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create()).handler(this::responseHandlers)
                .handler(ResponseContentTypeHandler.create()).failureHandler(this::errorHandler);

        long requestTimeout = bridgeServerConfig.getLong("requestTimeout", 5000L);
        long queryTimeout = Math.max(requestTimeout, bridgeServerConfig.getLong("queryTimeout", 120000L));

        // stateless endpoints
        router.get("/ping").handler(TimeoutHandler.create(requestTimeout)).handler(this::handlePing);
        router.post("/columns_info").produces(RESPONSE_CONTENT_TYPE).handler(TimeoutHandler.create(queryTimeout))
                .handler(this::handleColumnsInfo);
        router.post("/identifier_quote").produces(RESPONSE_CONTENT_TYPE).handler(TimeoutHandler.create(requestTimeout))
                .handler(this::handleIdentifierQuote);
        router.post("/").produces(RESPONSE_CONTENT_TYPE).handler(TimeoutHandler.create(queryTimeout))
                .handler(this::handleQuery);
        router.post("/write").produces(RESPONSE_CONTENT_TYPE).handler(TimeoutHandler.create(queryTimeout))
                .handler(this::handleWrite);

        log.info("Starting web server...");
        int port = bridgeServerConfig.getInteger("serverPort", DEFAULT_SERVER_PORT);
        server.requestHandler(router).listen(port, action -> {
            if (action.succeeded()) {
                log.info("Server http://localhost:{} started in {} ms", port, System.currentTimeMillis() - startTime);
            } else {
                log.error("Failed to start server", action.cause());
            }
        });
    }

    private void responseHandlers(RoutingContext ctx) {
        HttpServerRequest req = ctx.request();

        String path = ctx.normalisedPath();
        log.debug("[{}] Context:\n{}", path, ctx.data());
        log.debug("[{}] Headers:\n{}", path, req.headers());
        log.debug("[{}] Parameters:\n{}", path, req.params());
        log.trace("[{}] Body:\n{}", path, ctx.getBodyAsString());

        HttpServerResponse resp = ctx.response();

        resp.endHandler(handler -> {
            log.trace("[{}] About to end response...", ctx.normalisedPath());
        });

        resp.closeHandler(handler -> {
            log.trace("[{}] About to close response...", ctx.normalisedPath());
        });

        resp.drainHandler(handler -> {
            log.trace("[{}] About to drain response...", ctx.normalisedPath());
        });

        resp.exceptionHandler(throwable -> {
            log.error("Caught exception", throwable);
        });

        ctx.next();
    }

    private void errorHandler(RoutingContext ctx) {
        log.error("Failed to respond", ctx.failure());
        ctx.response().setStatusCode(500).end(ctx.failure().getMessage());
    }

    private void handlePing(RoutingContext ctx) {
        ctx.response().end(PING_RESPONSE);
    }

    private void handleColumnsInfo(RoutingContext ctx) {
        final QueryParser parser = QueryParser.fromRequest(ctx, datasources);

        String rawQuery = parser.getRawQuery();

        log.info("Raw query:\n{}", rawQuery);

        String uri = parser.getConnectionString();
        // boolean useNull =
        // Boolean.parseBoolean(req.getParam(PARAM_EXT_TABLE_USE_NULLS));

        QueryParameters params = parser.getQueryParameters();
        ClickHouseDataSource ds = datasources.get(uri, params.isDebug());
        String dsId = uri;
        if (ds != null) {
            dsId = ds.getId();
            params = ds.newQueryParameters(params);
        }

        final String columnsInfo;
        if (params.isDebug()) {
            columnsInfo = ClickHouseColumnList.DEFAULT_COLUMNS_INFO.toString();
        } else {
            // even it's a named query, the column list could be empty
            ClickHouseNamedQuery namedQuery = queries.get(rawQuery);
            ClickHouseColumnList columnList = namedQuery != null && namedQuery.hasColumn() ? namedQuery.getColumns()
                    : ds.getColumns(parser.getSchema(), parser.getNormalizedQuery());

            List<ClickHouseColumnInfo> additionalColumns = new ArrayList<ClickHouseColumnInfo>();
            if (params.showDatasourceColumn()) {
                additionalColumns.add(new ClickHouseColumnInfo(ClickHouseColumnList.COLUMN_DATASOURCE,
                        ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE, null, dsId));
            }
            if (params.showCustomColumns() && ds != null) {
                additionalColumns.addAll(ds.getCustomColumns());
            }

            if (additionalColumns.size() > 0) {
                columnList = new ClickHouseColumnList(columnList, true,
                        additionalColumns.toArray(new ClickHouseColumnInfo[0]));
            }

            columnsInfo = columnList.toString();
        }

        log.debug("Columns info:\n[{}]", columnsInfo);
        ctx.response().end(ClickHouseBuffer.asBuffer(columnsInfo));
    }

    private void handleIdentifierQuote(RoutingContext ctx) {
        String uri = QueryParser.extractConnectionString(ctx, datasources);
        ClickHouseDataSource ds = datasources.get(uri, true);

        // ds == null ? ClickHouseDataSource.DEFAULT_QUOTE_IDENTIFIER :
        ctx.response().end(ClickHouseBuffer.asBuffer(ds.getQuoteIdentifier()));
    }

    private void handleQuery(RoutingContext ctx) {
        final QueryParser parser = QueryParser.fromRequest(ctx, datasources);

        ctx.response().setChunked(true);

        vertx.executeBlocking(promise -> {
            log.trace("About to execute query...");

            QueryParameters params = parser.getQueryParameters();
            ClickHouseDataSource ds = datasources.get(parser.getConnectionString(), params.isDebug());
            params = ds == null ? params : ds.newQueryParameters(params);

            String generatedQuery = parser.getRawQuery();
            String normalizedQuery = parser.getNormalizedQuery();
            // try if it's a named query first
            ClickHouseNamedQuery namedQuery = queries.get(normalizedQuery);
            // in case the "query" is a local file...
            normalizedQuery = ds.loadSavedQueryAsNeeded(normalizedQuery);

            log.debug("Generated query:\n{}\nNormalized query:\n{}", generatedQuery, normalizedQuery);

            final HttpServerResponse resp = ctx.response();

            ClickHouseResponseWriter writer = new ClickHouseResponseWriter(resp, parser.getStreamOptions());

            if (params.isDebug()) {
                ClickHouseDataSource.writeDebugInfo(ds.getId(), ds.getType(),
                        ds.getColumns(parser.getSchema(), normalizedQuery), normalizedQuery, params, writer);
            } else {
                long executionStartTime = System.currentTimeMillis();
                if (namedQuery != null) {
                    log.debug("Found named query: [{}]", namedQuery);

                    // columns in request might just be a subset of defined list
                    // for example:
                    // - named query 'test' is: select a, b, c from table
                    // - clickhouse query: select b, a from jdbc('?','','test')
                    // - requested columns: b, a
                    ds.executeQuery(namedQuery, parser.getColumnList(), writer);
                } else {
                    // columnsInfo could be different from what we responded earlier, so let's parse
                    // it again
                    Boolean containsWhitespace = null;
                    for (int i = 0; i < normalizedQuery.length(); i++) {
                        char ch = normalizedQuery.charAt(i);
                        if (Character.isWhitespace(ch)) {
                            if (containsWhitespace != null) {
                                containsWhitespace = Boolean.TRUE;
                                break;
                            }
                        } else if (containsWhitespace == null) {
                            containsWhitespace = Boolean.FALSE;
                        }
                    }

                    ClickHouseColumnList queryColumns = parser.getColumnList();
                    // unfortunately default values will be lost between two requests, so we have to
                    // add it back...
                    List<ClickHouseColumnInfo> additionalColumns = new ArrayList<ClickHouseColumnInfo>();
                    if (params.showDatasourceColumn()) {
                        additionalColumns.add(new ClickHouseColumnInfo(ClickHouseColumnList.COLUMN_DATASOURCE,
                                ClickHouseDataType.String, true, DEFAULT_PRECISION, DEFAULT_SCALE, null, ds.getId()));
                    }
                    if (params.showCustomColumns()) {
                        additionalColumns.addAll(ds.getCustomColumns());
                    }

                    queryColumns.updateValues(additionalColumns);
                    ds.executeQuery(Boolean.TRUE.equals(containsWhitespace) ? normalizedQuery : generatedQuery,
                            queryColumns, params, writer);
                }

                log.debug("Completed execution in {} ms.", System.currentTimeMillis() - executionStartTime);
            }

            promise.complete();
        }, false, res -> {
            if (res.succeeded()) {
                log.debug("Wrote back query result");
                ctx.response().end();
            } else {
                ctx.fail(res.cause());
            }
        });
    }

    // https://github.com/ClickHouse/ClickHouse/blob/bee5849c6a7dba20dbd24dfc5fd5a786745d90ff/programs/odbc-bridge/MainHandler.cpp#L169
    private void handleWrite(RoutingContext ctx) {
        final QueryParser parser = QueryParser.fromRequest(ctx, datasources, true);

        ctx.response().setChunked(true);

        vertx.executeBlocking(promise -> {
            log.trace("About to execute mutation...");

            QueryParameters params = parser.getQueryParameters();
            ClickHouseDataSource ds = datasources.get(parser.getConnectionString(), params.isDebug());
            params = ds == null ? params : ds.newQueryParameters(params);

            final HttpServerRequest req = ctx.request();
            final HttpServerResponse resp = ctx.response();

            final String generatedQuery = parser.getRawQuery();

            String normalizedQuery = parser.getNormalizedQuery();

            // try if it's a named query first
            ClickHouseNamedQuery namedQuery = queries.get(normalizedQuery);
            // in case the "query" is a local file...
            normalizedQuery = ds.loadSavedQueryAsNeeded(normalizedQuery);

            log.debug("Generated query:\n{}\nNormalized query:\n{}", generatedQuery, normalizedQuery);

            // req.pipeTo(null);
            resp.write(ClickHouseBuffer.asBuffer(WRITE_RESPONSE));

            promise.complete();
        }, false, res -> {
            if (res.succeeded()) {
                log.debug("Wrote back query result");
                ctx.response().end();
            } else {
                ctx.fail(res.cause());
            }
        });
    }

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();

        // https://github.com/eclipse-vertx/vert.x/blob/master/src/main/generated/io/vertx/core/VertxOptionsConverter.java
        Vertx vertx = Vertx.vertx(new VertxOptions(ClickHouseUtils.loadJsonFromFile(CONFIG_PATH + "/vertx.json")));

        vertx.deployVerticle(new DataSourceBridgeVerticle());
    }
}
