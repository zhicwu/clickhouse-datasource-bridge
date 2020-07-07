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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import io.vertx.core.json.JsonObject;

public class ClickHouseUtilsTest {
    @Test(groups = { "unit" })
    public void testDigest() {
        JsonObject obj = new JsonObject("{\"b\": {\"x\": \"y\"}, \"a\": 1}");

        String nullString = null;
        String emptyString = "";
        String jsonString = obj.encode();

        assertEquals(ClickHouseUtils.digest(nullString), ClickHouseUtils.EMPTY_STRING);
        assertEquals(ClickHouseUtils.digest(emptyString), ClickHouseUtils.EMPTY_STRING);
        assertEquals(ClickHouseUtils.digest(jsonString),
                "4fe27ca694db4391c6b2f199b24b64b5d632f8f94a4992e95350ee05e9150d04feba17d7486ff487e7fd2f5f7884d1c34d0816305c971bc22cb63c9e24e085bd");

        assertEquals(ClickHouseUtils.digest(obj), ClickHouseUtils.digest(jsonString));
        obj.remove("a");
        obj.put("a", 1);
        assertEquals(ClickHouseUtils.digest(obj), ClickHouseUtils.digest(jsonString));
        obj.put("a", "1");
        assertNotEquals(ClickHouseUtils.digest(obj), ClickHouseUtils.digest(jsonString));
    }

    @Test(groups = { "unit" })
    public void testSplitByChar() {
        String str = "a=1&b=2&& c=3&";
        char delimiter = '&';

        assertEquals(ClickHouseUtils.splitByChar(str, delimiter), ClickHouseUtils.splitByChar(str, delimiter, true));

        List<String> matches = ClickHouseUtils.splitByChar(str, delimiter, false);
        assertEquals(String.join(String.valueOf(delimiter), matches), str);
    }

    @Test(groups = { "unit" })
    public void testLoadJson() {
        JsonObject expected = new JsonObject("{\"b\": {\"x\": \"y\"}, \"a\": 1}");

        JsonObject obj = ClickHouseUtils.loadJsonFromFile("src/test/resources/test.json");
        assertEquals(obj.encode(), expected.encode());

        // now try a file does not exist...
        obj = ClickHouseUtils.loadJsonFromFile("src/test/resources/file_does_not_exist.json");
        assertNotNull(obj);
    }

    @Test(groups = { "unit" })
    public void testApplyVariables() {
        String nullTemplate = null;
        String emptyTemplate = "";
        String templateWithoutVariable = "test template without any variable";
        String var0Template = ClickHouseUtils.VARIABLE_PREFIX + ClickHouseUtils.VARIABLE_SUFFIX;
        String var1Template = "template: {{ var #1 }}";
        String var2Template = "{{var2}}";
        String var3Template = "template: {{{{var3}}}}";
        String var4Template = "{{var2}} {{ {{var3 {{var #1}}";

        Map<String, String> nullMap = null;
        Map<String, String> emptyMap = new HashMap<String, String>();
        Map<String, String> varsMap = new HashMap<String, String>();
        varsMap.put("var #1", "value 1");
        varsMap.put("var2", "value 2");
        varsMap.put("{{var3", "value 3");

        assertEquals(ClickHouseUtils.applyVariables(nullTemplate, nullMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(nullTemplate, emptyMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(emptyTemplate, nullMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(emptyTemplate, emptyMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(nullTemplate, varsMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(emptyTemplate, varsMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(ClickHouseUtils.VARIABLE_PREFIX, varsMap),
                ClickHouseUtils.VARIABLE_PREFIX);

        assertEquals(ClickHouseUtils.applyVariables(emptyTemplate, nullMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(emptyTemplate, emptyMap), emptyTemplate);
        assertEquals(ClickHouseUtils.applyVariables(templateWithoutVariable, varsMap), templateWithoutVariable);

        assertEquals(ClickHouseUtils.applyVariables(var0Template, varsMap), var0Template);
        assertEquals(ClickHouseUtils.applyVariables(var1Template, varsMap), "template: value 1");
        assertEquals(ClickHouseUtils.applyVariables(var2Template, varsMap), "value 2");
        assertEquals(ClickHouseUtils.applyVariables(var3Template, varsMap), "template: value 3}}");
        assertEquals(ClickHouseUtils.applyVariables(var4Template, varsMap), "value 2 {{ {{var3 value 1");
    }
}