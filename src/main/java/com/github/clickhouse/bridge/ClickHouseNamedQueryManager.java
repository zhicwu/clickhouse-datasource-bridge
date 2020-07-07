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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.github.clickhouse.bridge.core.ClickHouseNamedQuery;

import io.vertx.core.json.JsonObject;

public class ClickHouseNamedQueryManager {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClickHouseNamedQueryManager.class);

    private final Map<String, ClickHouseNamedQuery> mappings = new HashMap<>();

    protected ClickHouseNamedQueryManager() {
    }

    protected void update(String id, JsonObject config) {
        ClickHouseNamedQuery query = mappings.get(id);

        boolean addQuery = false;
        if (query == null) {
            addQuery = true;
        } else if (query.isDifferentFrom(config)) {
            mappings.remove(id);
            addQuery = true;
        }

        if (addQuery && config != null) {
            log.info("Adding query [{}]...", id);
            try {
                mappings.put(id, new ClickHouseNamedQuery(id, config));
            } catch (Exception e) {
                log.error("Failed to add query", e);
            }
        }
    }

    public void reload(JsonObject config) {
        if (config == null || config.fieldNames().size() == 0) {
            log.info("No query configuration found, which is fine");
            mappings.clear();
        } else {
            HashSet<String> keys = new HashSet<>();
            config.forEach(action -> {
                String id = action.getKey();
                if (id != null) {
                    keys.add(id);
                    update(id, action.getValue() instanceof JsonObject ? (JsonObject) action.getValue() : null);
                }
            });

            mappings.entrySet().removeIf(entry -> {
                boolean shouldRemove = !keys.contains(entry.getKey());

                if (shouldRemove) {
                    log.info("Removing query [{}]...", entry.getKey());
                }

                return shouldRemove;
            });
        }
    }

    public ClickHouseNamedQuery get(String query) {
        return mappings.get(query);
    }
}