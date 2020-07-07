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

import static org.testng.Assert.*;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class ClickHouseDataSourceTest {
    static class DummyDataSourceResolver implements IDataSourceResolver {
        @Override
        public String resolve(String uri) {
            return uri;
        }
    }

    @Test(groups = { "unit" })
    public void testConstructor() {
        String dataSourceId = "test-datasource";
        JsonObject config = ClickHouseUtils.loadJsonFromFile("src/test/resources/datasources/test-datasource.json");

        ClickHouseDataSource ds = new ClickHouseDataSource(dataSourceId, new DummyDataSourceResolver(),
                config.getJsonObject(dataSourceId));
        assertEquals(ds.getId(), dataSourceId);
        for (ClickHouseColumnInfo col : ds.getCustomColumns()) {
            assertEquals(col.getName(), "c_" + col.getType().name().toLowerCase());
            switch (col.getType()) {
                case Decimal:
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Float32:
                case Float64:
                    assertEquals(String.valueOf(col.getValue()), "2.0");
                    break;
                default:
                    assertEquals(String.valueOf(col.getValue()), "2");
                    break;
            }
        }

        for (ClickHouseDataType type : ClickHouseDataType.values()) {
            Object value = ds.getDefaultValues().getTypedValue(type).getValue();
            switch (type) {
                case Decimal:
                case Decimal32:
                case Decimal64:
                case Decimal128:
                case Float32:
                case Float64:
                    assertEquals(String.valueOf(value), "3.0");
                    break;
                default:
                    assertEquals(String.valueOf(value), "3");
                    break;
            }
        }
    }

    @Test(groups = { "unit" })
    public void testGetColumns() {
        String dataSourceId = "test-datasource";
        JsonObject config = ClickHouseUtils.loadJsonFromFile("src/test/resources/datasources/test-datasource.json");

        ClickHouseDataSource ds = new ClickHouseDataSource(dataSourceId, new DummyDataSourceResolver(),
                config.getJsonObject(dataSourceId));
        ds.getColumns("", "src/test/resources/simple.query");
        assertEquals(ds.getId(), dataSourceId);
    }
}