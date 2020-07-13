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

public class QueryParserTest {
    @Test(groups = { "unit" })
    public void testNormalizeQuery() {
        String query = "some_table";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "some named query";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "SELECT col1, col2 FROM some_table";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "SELECT col1, col2 FROM some_schema.some_table";
        assertEquals(QueryParser.normalizeQuery(query), query);

        query = "SELECT `col1`, `col2` FROM `some_table`";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        query = "SELECT `col1`, `col2` FROM `some_schema`.`some_table`";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        query = "SELECT \"col1\", \"col2\" FROM \"some_table\"";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        query = "SELECT \"col1\", \"col2\" FROM \"some_schema\".\"some_table\"";
        assertEquals(QueryParser.normalizeQuery(query), "some_table");

        String embeddedQuery = "select 1";
        query = "SELECT `col1`, `col2` FROM `" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT `col1`, `col2` FROM `some_schema`.`" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"some_schema\".\"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        embeddedQuery = "select 's' as s";

        query = "SELECT `s` FROM `" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT `s` FROM `" + embeddedQuery + "` WHERE `s` = 's'";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        embeddedQuery = "select\t'1`2\"3'\r\n, -- `\"\n/* \"s` */'`''`'";
        query = "SELECT `col1`, `col2` FROM `" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT `col1`, `col2` FROM `some_schema`.`" + embeddedQuery + "`";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);

        query = "SELECT \"col1\", \"col2\" FROM \"some_schema\".\"" + embeddedQuery + "\"";
        assertEquals(QueryParser.normalizeQuery(query), embeddedQuery);
    }

    @Test(groups = { "unit" })
    public void testExtractTableName() {
        assertEquals(QueryParser.extractTableName(null), null);
        assertEquals(QueryParser.extractTableName(""), "");
        assertEquals(QueryParser.extractTableName("a"), "a");
        assertEquals(QueryParser.extractTableName("a.a"), "a.a");

        String table = "`schema`.`table`";
        assertEquals(QueryParser.extractTableName("SELECT * FROM " + table), table);
        assertEquals(QueryParser.extractTableName("SELECT * from " + table), table);
        assertEquals(QueryParser.extractTableName("SELECT * FROM  " + table + " where col1=11"), table);
        assertEquals(QueryParser.extractTableName("SELECT * FROM\r" + table + " where col1=11"), table);
        assertEquals(QueryParser.extractTableName("SELECT * FROM (select col1 from " + table + " where col1=11) a"),
                table);
        assertEquals(QueryParser.extractTableName(
                "SELECT col1, ' from b' as a FROM (select col1 from " + table + " where col1=11) a"), table);
    }
}