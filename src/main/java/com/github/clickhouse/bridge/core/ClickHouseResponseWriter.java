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

import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerResponse;

public class ClickHouseResponseWriter {
    private final HttpServerResponse response;
    private final StreamOptions options;

    public ClickHouseResponseWriter(HttpServerResponse response, StreamOptions options) {
        this.response = response;
        this.options = options;

        this.response.setWriteQueueMaxSize(this.options.getMaxBlockSize());
    }

    public StreamOptions getOptions() {
        return this.options;
    }

    public boolean isOpen() {
        return !this.response.closed() && !this.response.ended();
    }

    public void setDrainHanlder(Handler<Void> handler) {
        this.response.drainHandler(handler);
    }

    public void write(ClickHouseBuffer buffer) {
        if (this.response.closed() || this.response.ended()) {
            throw new IllegalStateException("Response stream was closed");
        }

        this.response.write(buffer.unwrap());
    }
}