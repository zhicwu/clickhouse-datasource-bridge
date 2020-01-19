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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.github.clickhouse.bridge.core.ClickHouseDataSource;
import com.github.clickhouse.bridge.jdbc.ClickHouseJdbcDataSource;

import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseDataSourceManager {
    private static final String CONF_TYPE = "type";
    private static final String CONF_JDBC_URL = "jdbcUrl";

    private final Map<String, Constructor<ClickHouseDataSource>> types = new HashMap<>();
    private final Map<String, ClickHouseDataSource> mappings = new HashMap<>();

    protected ClickHouseDataSourceManager() {
        this.registerType(ClickHouseJdbcDataSource.DATASOURCE_TYPE, ClickHouseJdbcDataSource.class.getName());
    }

    protected void registerType(String typeName, String className) {
        log.info("Registering new type of data source: [{}] -> [{}]", typeName, className);

        try {
            Class<ClickHouseDataSource> clazz = (Class<ClickHouseDataSource>) this.getClass().getClassLoader()
                    .loadClass(className);
            Constructor<ClickHouseDataSource> constructor = clazz.getConstructor(String.class, JsonObject.class);

            types.put(typeName, constructor);
        } catch (Exception e) {
            log.error("Failed to register new type of data source", e);
        }
    }

    private ClickHouseDataSource createFromType(String uri, String type, boolean nonNullRequired) {
        ClickHouseDataSource ds = null;

        if (type != null) {
            try {
                Constructor<ClickHouseDataSource> constructor = types.get(type);

                if (constructor != null) {
                    ds = constructor.newInstance(uri, null);
                }
            } catch (Exception e) {
                log.error("Failed to create data source [" + uri + "]", e);
            }
        }

        return ds == null && nonNullRequired ? new ClickHouseDataSource(uri, null) : ds;
    }

    /**
     * Create datasource object based on given configuration.
     * 
     * @param config configuration in JSON format
     * @return desired datasource
     */
    protected ClickHouseDataSource createFromConfig(String id, JsonObject config) {
        ClickHouseDataSource ds = null;

        if (config != null) {
            ds = createFromType(id, config.getString(CONF_TYPE), false);

            // could it be JDBC data source?
            if (ds == null && config.containsKey(CONF_JDBC_URL)) {
                ds = new ClickHouseJdbcDataSource(id, config);
            }
        }

        // fall back to default implementation
        if (ds == null) {
            ds = new ClickHouseDataSource(id, config);
        }

        return ds;
    }

    protected void update(String id, JsonObject config) {
        ClickHouseDataSource ds = mappings.remove(id);
        if (ds != null) {
            // TODO check if there's any important change(e.g. jdbcUrl for Database) first
            try {
                ds.close();
            } catch (IOException e) {
            }
        }

        if (config != null) {
            log.info("Adding datasource [{}]...", id);

            try {
                mappings.put(id, createFromConfig(id, config));
            } catch (Exception e) {
                log.warn("Failed to add datasource", e);
            }
        }
    }

    public void registerTypes(JsonObject config) {
        if (config != null) {
            config.forEach(action -> {
                String typeName = action.getKey();
                Object value = action.getValue();
                if (value instanceof String) {
                    registerType(typeName, (String) value);
                }
            });
        }
    }

    public void reload(JsonObject config) {
        if (config == null || config.fieldNames().size() == 0) {
            log.info("No datasource configuration found, which is fine");

            mappings.entrySet().forEach(mapping -> {
                log.info("Removing datasource [{}]...", mapping.getKey());
                try {
                    mapping.setValue(null).close();
                } catch (IOException e) {
                }
            });

            mappings.clear();
        } else {
            HashSet<String> keys = new HashSet<>();
            config.forEach(entry -> {
                String id = entry.getKey();
                if (id != null) {
                    keys.add(id);
                    update(id, entry.getValue() instanceof JsonObject ? (JsonObject) entry.getValue() : null);
                }
            });

            Iterator<Map.Entry<String, ClickHouseDataSource>> entryIt = mappings.entrySet().iterator();
            while (entryIt.hasNext()) {
                Map.Entry<String, ClickHouseDataSource> entry = entryIt.next();
                if (!keys.contains(entry.getKey())) {
                    log.info("Removing datasource [{}]...", entry.getKey());
                    try {
                        entry.setValue(null).close();
                    } catch (IOException e) {
                    }
                    entryIt.remove();
                }
            }
        }
    }

    /**
     * Get or create a data source from given URI.
     * 
     * @param uri
     * @param orCreate
     * @return
     */
    public ClickHouseDataSource get(String uri, boolean orCreate) {
        // [<type>:]<id or connection string>[?<query parameters>]
        String id = uri;
        String type = null;
        if (id != null) {
            // remove query parameters first
            int index = id.indexOf('?');
            if (index >= 0) {
                id = id.substring(0, index);
            }

            // and then type prefix
            index = id.indexOf(':');
            if (index >= 0) {
                type = id.substring(0, index);
                id = id.substring(index + 1);
            }

            // now try parsing it as URI
            try {
                URI u = new URI(id);
                if (u.getHost() != null) {
                    id = u.getHost();
                }
            } catch (Exception e) {
            }
        }

        ClickHouseDataSource ds = mappings.get(id);

        if (ds == null && (ds = createFromType(uri, type, orCreate)) == null) {
            throw new IllegalArgumentException("Data source [" + uri + "] not found!");
        }

        return ds;
    }
}