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

import static org.testng.Assert.*;

import com.github.clickhouse.bridge.core.ClickHouseDataSource;

import org.testng.annotations.Test;

public class ClickHouseDataSourceManagerTest {
    @Test(groups = { "unit" }, expectedExceptions = { IllegalArgumentException.class })
    public void testGetException() throws Exception {
        ClickHouseDataSourceManager manager = new ClickHouseDataSourceManager();
        assertNull(manager.get("non-existing data source", false));
    }

    @Test(groups = { "unit" })
    public void testGet() {
        ClickHouseDataSourceManager manager = new ClickHouseDataSourceManager();

        String uri = "some invalid uri";
        ClickHouseDataSource ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);

        uri = "jdbc:mysql://localhost:3306/test?useSSL=false";
        ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);

        uri = "jdbc:weird:vendor:hostname:1234?database=test";
        ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);

        uri = "jenkins:https://my.ci-server.org/internal/";
        ds = manager.get(uri, true);
        assertNotNull(ds);
        assertEquals(ds.getId(), uri);
    }

    @Test(groups = { "sit" })
    public void testSrvRecordSupport() {
        ClickHouseDataSourceManager manager = new ClickHouseDataSourceManager();

        String host = "_sip._udp.sip.voice.google.com";
        String port = "5060";
        String hostAndPort = host + ":" + port;

        assertEquals(manager.resolve("jdbc://{{ _sip._udp.sip.voice.google.com }}/aaa"),
                "jdbc://" + hostAndPort + "/aaa");
    }
}