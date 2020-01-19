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

public class ClickHouseColumnInfoTest {
    @Test(groups = { "unit" })
    public void testConstructor() {
        String weirdName = "123`abc!$";
        ClickHouseDataType weirdType = ClickHouseDataType.DateTime;
        boolean nullableOrNot = false;
        ClickHouseColumnInfo c = new ClickHouseColumnInfo(weirdName, weirdType, nullableOrNot, 3, 1);

        assertEquals(c.getName(), weirdName);
        assertEquals(c.getType(), weirdType);
        assertEquals(c.isNullable(), nullableOrNot);
        assertEquals(c.getPrecision(), 3);
        assertEquals(c.getScale(), 1);
    }

    @Test(groups = { "unit" })
    public void testFromJson() {
        String name = "column1";
        ClickHouseDataType type = ClickHouseDataType.String;
        boolean nullable = true;

        ClickHouseColumnInfo c = new ClickHouseColumnInfo(name, type, nullable, 0, 0);

        JsonObject json = new JsonObject();
        assertEquals(ClickHouseColumnInfo.fromJson(json).getName(), ClickHouseColumnInfo.DEFAULT_NAME);
        assertEquals(ClickHouseColumnInfo.fromJson(json).getType(), ClickHouseColumnInfo.DEFAULT_TYPE);
        assertEquals(ClickHouseColumnInfo.fromJson(json).isNullable(), ClickHouseColumnInfo.DEFAULT_NULLABLE);
        json.put("name", "");
        assertEquals(ClickHouseColumnInfo.fromJson(json).getName(), "");
        json.put("name", name);
        assertEquals(ClickHouseColumnInfo.fromJson(json).getName(), c.getName());
        json.put("type", ClickHouseDataType.Date.name());
        assertEquals(ClickHouseColumnInfo.fromJson(json).getType(), ClickHouseDataType.Date);
        json.put("type", type.name());
        assertEquals(ClickHouseColumnInfo.fromJson(json).getType(), c.getType());
        json.put("nullable", !nullable);
        assertEquals(ClickHouseColumnInfo.fromJson(json).isNullable(), !nullable);
        json.put("nullable", nullable);
        assertEquals(ClickHouseColumnInfo.fromJson(json), c);
    }

    @Test(groups = { "unit" })
    public void testFromString() {
        String name = "column`1";
        ClickHouseDataType type = ClickHouseDataType.String;
        boolean nullable = true;

        ClickHouseColumnInfo c = new ClickHouseColumnInfo(name, type, nullable, 0, 0);
        String declarationWithQuote = "`column``1` Nullable(String)";
        String declarationWithoutQuote = "column`1 Nullable(String)";

        assertEquals(ClickHouseColumnInfo.fromString(declarationWithQuote), c);
        assertEquals(ClickHouseColumnInfo.fromString(declarationWithoutQuote), c);

        assertEquals(ClickHouseColumnInfo.fromString("cloumn1").getType(), type);
        assertEquals(ClickHouseColumnInfo.fromString("cloumn1 ").getType(), type);
        assertEquals(ClickHouseColumnInfo.fromString("cloumn1 String").getType(), type);
        assertEquals(ClickHouseColumnInfo.fromString("cloumn1 String").isNullable(), false);

        // now try some weird names
        String weirdName = "``cl`o``u`mn``";
        assertEquals(ClickHouseColumnInfo.fromString("`````cl``o````u``mn`````").getName(), weirdName);
    }

    @Test(groups = { "unit" })
    public void testDecimals() {
        ClickHouseColumnInfo c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal, true, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal(10,3))");

        c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal32, true, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal32(3))");

        c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal64, true, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal64(3))");

        c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal128, true, 10, 3);
        assertEquals(c.toString(), "`d` Nullable(Decimal128(3))");

        // incorrect scale
        c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal, true, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal(10,10))");

        c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal32, true, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal32(9))");

        c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal64, true, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal64(18))");

        c = new ClickHouseColumnInfo("d", ClickHouseDataType.Decimal128, true, 10, 50);
        assertEquals(c.toString(), "`d` Nullable(Decimal128(38))");

        String types = "`d` Nullable(Decimal(7,3))";
        assertEquals(ClickHouseColumnInfo.fromString(types).toString(), types);
        types = "`d` Nullable(Decimal64(8))";
        assertEquals(ClickHouseColumnInfo.fromString(types).toString(), types);
    }
}