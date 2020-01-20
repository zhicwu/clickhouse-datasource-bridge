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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.github.clickhouse.bridge.core.ClickHouseBuffer;
import com.github.clickhouse.bridge.core.ClickHouseColumnList;
import com.github.clickhouse.bridge.core.ClickHouseDataSource;
import com.github.clickhouse.bridge.core.ClickHouseNamedQuery;
import com.github.clickhouse.bridge.core.ClickHouseResponseWriter;
import com.github.clickhouse.bridge.core.QueryParameters;
import com.github.clickhouse.bridge.core.StreamOptions;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.ext.web.handler.TimeoutHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * Unified data source bridge for ClickHouse.
 *
 * @author Zhichun Wu
 */
@Slf4j
public class DataSourceBridgeVerticle extends AbstractVerticle {
    private static long startTime;

    private static final String CONFIG_PATH = "config";

    private static final int DEFAULT_SERVER_PORT = 9019;

    private static final String RESPONSE_CONTENT_TYPE = "application/octet-stream";

    private static final String PARAM_CONNECTION_STRING = "connection_string";
    private static final String PARAM_SCHEMA = "schema";
    private static final String PARAM_TABLE = "table";
    private static final String PARAM_COLUMNS = "columns";
    private static final String PARAM_QUERY = "query";

    private static final String EXPR_QUERY = PARAM_QUERY + "=";
    private static final String EXPR_FROM = " FROM ";

    private static final String PING_RESPONSE = "Ok.\n";

    private final ClickHouseDataSourceManager datasources = new ClickHouseDataSourceManager();
    private final ClickHouseNamedQueryManager queries = new ClickHouseNamedQueryManager();

    @Override
    public void start() {
        JsonObject config = loadJsonObject(CONFIG_PATH + "/server.json");

        datasources.registerTypes(config.getJsonObject("datasources"));

        long scanPeriod = config.getLong("configScanPeriod", 5000L);

        initConfig(CONFIG_PATH + "/datasources", scanPeriod, datasources::reload);
        initConfig(CONFIG_PATH + "/queries", scanPeriod, queries::reload);

        startServer(config);
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

    private void startServer(JsonObject config) {
        HttpServer server = vertx.createHttpServer();
        // vertx.createHttpServer(new
        // HttpServerOptions().setTcpNoDelay(false).setTcpKeepAlive(true)
        // .setTcpFastOpen(true).setLogActivity(true));

        // https://github.com/vert-x3/vertx-examples/blob/master/web-examples/src/main/java/io/vertx/example/web/mongo/Server.java
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create()).handler(this::responseHandlers)
                .handler(ResponseContentTypeHandler.create()).failureHandler(this::errorHandler);

        long requestTimeout = config.getLong("requestTimeout", 5000L);
        long queryTimeout = Math.max(requestTimeout, config.getLong("queryTimeout", 120000L));

        // stateless endpoints
        router.get("/ping").handler(TimeoutHandler.create(requestTimeout)).handler(this::handlePing);
        router.post("/columns_info").produces(RESPONSE_CONTENT_TYPE).handler(TimeoutHandler.create(queryTimeout))
                .handler(this::handleColumnsInfo);
        router.post("/identifier_quote").produces(RESPONSE_CONTENT_TYPE).handler(TimeoutHandler.create(requestTimeout))
                .handler(this::handleIdentifierQuote);
        router.post("/").produces(RESPONSE_CONTENT_TYPE).handler(TimeoutHandler.create(queryTimeout))
                .handler(this::handleQuery);

        log.info("Starting web server...");
        int port = config.getInteger("serverPort", DEFAULT_SERVER_PORT);
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
        log.debug("[{}] Body:\n{}", path, ctx.getBodyAsString());
        log.debug("[{}] Headers:\n{}", path, req.headers());
        log.debug("[{}] Parameters:\n{}", path, req.params());

        HttpServerResponse resp = ctx.response();

        resp.endHandler(handler -> {
            log.debug("[{}] About to end response...", ctx.normalisedPath());
        });

        resp.closeHandler(handler -> {
            log.debug("[{}] About to close response...", ctx.normalisedPath());
        });

        resp.drainHandler(handler -> {
            log.debug("[{}] About to drain response...", ctx.normalisedPath());
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
        HttpServerRequest req = ctx.request();

        String rawQuery = req.getParam(PARAM_TABLE);

        log.info("Raw query:\n{}", rawQuery);

        String uri = req.getParam(PARAM_CONNECTION_STRING);

        QueryParameters params = new QueryParameters(uri);
        ClickHouseDataSource ds = datasources.get(uri, params.isDebug());
        params = ds == null ? params : new QueryParameters(ds.getQueryParameters()).merge(params);

        final String columnsInfo;
        if (params.isDebug()) {
            columnsInfo = ClickHouseColumnList.DEFAULT_COLUMNS_INFO.toString();
        } else {
            // even it's a named query, the column list could be empty
            ClickHouseNamedQuery namedQuery = queries.get(rawQuery);
            columnsInfo = namedQuery != null && namedQuery.hasColumn() ? namedQuery.getColumns().toString()
                    : ds.getColumns(req.getParam(PARAM_SCHEMA), normalizeQuery(rawQuery));
        }

        log.debug("Columns info:\n[{}]", columnsInfo);
        ctx.response().end(ClickHouseBuffer.asBuffer(columnsInfo));
    }

    private void handleIdentifierQuote(RoutingContext ctx) {
        ClickHouseDataSource ds = datasources.get(ctx.request().getParam(PARAM_CONNECTION_STRING), true);

        ctx.response().end(ClickHouseBuffer
                .asBuffer(ds == null ? ClickHouseDataSource.DEFAULT_QUOTE_IDENTIFIER : ds.getQuoteIdentifier()));
    }

    private void handleQuery(RoutingContext ctx) {
        final String uri = ctx.request().getParam(PARAM_CONNECTION_STRING);

        ctx.response().setChunked(true);

        vertx.executeBlocking(promise -> {
            log.debug("About to execute query...");

            QueryParameters params = new QueryParameters(uri);
            ClickHouseDataSource ds = datasources.get(uri, params.isDebug());
            params = ds == null ? params : new QueryParameters(ds.getQueryParameters()).merge(params);

            String generatedQuery = ctx.getBodyAsString();
            // remove optional prefix
            if (generatedQuery != null && generatedQuery.startsWith(EXPR_QUERY)) {
                generatedQuery = generatedQuery.substring(EXPR_QUERY.length());
            }

            String normalizedQuery = normalizeQuery(generatedQuery);
            log.debug("Generated query:\n{}\nNormalized query:\n{}", generatedQuery, normalizedQuery);

            final HttpServerRequest req = ctx.request();
            final HttpServerResponse resp = ctx.response();

            ClickHouseResponseWriter writer = new ClickHouseResponseWriter(resp, new StreamOptions(req.params()));

            if (params.isDebug()) {
                ClickHouseDataSource.writeDebugInfo(ds.getId(), ds.getType(), normalizedQuery, params, writer);
            } else {
                // try if it's a named query first
                ClickHouseNamedQuery namedQuery = queries.get(normalizedQuery);

                long executionStartTime = System.currentTimeMillis();
                if (namedQuery != null) {
                    log.debug("Found named query: [{}]", namedQuery);

                    // columns in request might just be a subset of defined list
                    // for example:
                    // - named query 'test' is: select a, b, c from table
                    // - clickhouse query: select b, a from jdbc('?','','test')
                    // - requested columns: b, a
                    ds.execute(namedQuery, ClickHouseColumnList.fromString(req.getParam(PARAM_COLUMNS)), writer);
                } else {
                    // columnsInfo could be different from what we responded earlier, so let's parse
                    // it again
                    ds.execute(normalizedQuery.indexOf(' ') > 0 ? normalizedQuery : generatedQuery,
                            ClickHouseColumnList.fromString(req.getParam(PARAM_COLUMNS)), params, writer);
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

    static String normalizeQuery(String query) {
        Objects.requireNonNull(query);

        String normalizedQuery = query;

        // since we checked if this could be a named query before calling this method,
        // we know the extracted query will be either a table name or an adhoc query
        String extractedQuery = null;
        int index = query.indexOf(EXPR_FROM);
        if (index > 0 && query.length() > (index = index + EXPR_FROM.length())) {
            // assume quote is just one character and it always exists
            char quote = query.charAt(index++);

            int dotIndex = query.indexOf('.', index);

            if (dotIndex > index && query.length() > dotIndex && query.charAt(dotIndex - 1) == quote
                    && query.charAt(dotIndex + 1) == quote) { // has schema
                dotIndex += 2;
                int endIndex = query.lastIndexOf(quote);
                if (endIndex > dotIndex) {
                    extractedQuery = query.substring(dotIndex, endIndex);
                }
            } else if (quote == '"' || quote == '`') {
                int endIndex = query.lastIndexOf(quote);
                if (endIndex > index) {
                    extractedQuery = query.substring(index, endIndex);
                }
            }
        }

        normalizedQuery = extractedQuery != null ? extractedQuery.trim() : normalizedQuery.trim();

        // unescape String is mission impossible so we only considered below ones:
        // \t Insert a tab in the text at this point.
        // \b Insert a backspace in the text at this point.
        // \n Insert a newline in the text at this point.
        // \r Insert a carriage return in the text at this point.
        // \f Insert a formfeed in the text at this point.
        // \' Insert a single quote character in the text at this point.
        // \" Insert a double quote character in the text at this point.
        // \\ Insert a backslash character in the text at this point.
        StringBuilder builder = new StringBuilder();
        for (int i = 0, len = normalizedQuery.length(); i < len; i++) {
            char ch = normalizedQuery.charAt(i);
            if (ch == '\\' && i + 1 < len) {
                char nextCh = normalizedQuery.charAt(i + 1);
                switch (nextCh) {
                case 't':
                    builder.append('\t');
                    i++;
                    break;
                case 'b':
                    builder.append('\b');
                    i++;
                    break;
                case 'n':
                    builder.append('\n');
                    i++;
                    break;
                case 'r':
                    builder.append('\r');
                    i++;
                    break;
                case 'f':
                    builder.append('\f');
                    i++;
                    break;
                case '\'':
                    builder.append('\'');
                    i++;
                    break;
                case '"':
                    builder.append('"');
                    i++;
                    break;
                case '\\':
                    builder.append('\\');
                    i++;
                    break;
                default:
                    builder.append(ch);
                    break;
                }
            } else {
                builder.append(ch);
            }
        }

        return builder.toString().trim();
    }

    private static JsonObject loadJsonObject(String file) {
        log.info("Loading configuration from [{}]...", file);

        JsonObject config = null;

        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(file), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append('\n'));
            config = new JsonObject(contentBuilder.toString());
        } catch (Exception e) {
            log.warn("Failed to read json config", e);
        }

        return config == null ? new JsonObject() : config;
    }

    public static void main(String[] args) {
        startTime = System.currentTimeMillis();

        // https://github.com/eclipse-vertx/vert.x/blob/master/src/main/generated/io/vertx/core/VertxOptionsConverter.java
        Vertx vertx = Vertx.vertx(new VertxOptions(loadJsonObject(CONFIG_PATH + "/vertx.json")));

        vertx.deployVerticle(new DataSourceBridgeVerticle());
    }
}
