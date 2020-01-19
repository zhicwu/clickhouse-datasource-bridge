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

import java.util.List;

import org.testng.annotations.Test;

public class ClickHouseBufferTest {
    @Test(groups = { "unit" })
    public void testSplitByChar() {
        String str = "a=1&b=2&& c=3&";
        char delimiter = '&';

        assertEquals(ClickHouseBuffer.splitByChar(str, delimiter), ClickHouseBuffer.splitByChar(str, delimiter, true));

        List<String> matches = ClickHouseBuffer.splitByChar(str, delimiter, false);
        assertEquals(String.join(String.valueOf(delimiter), matches), str);
    }

    @Test(groups = { "unit" })
    public void testWriteInt8() {
        ClickHouseBuffer buffer = ClickHouseBuffer.newInstance(100);

        buffer.writeInt8(Byte.MIN_VALUE);
        buffer.writeInt8(Byte.MAX_VALUE);
        buffer.writeInt8(0xff);
        assertEquals(buffer.buffer.getBytes(), new byte[] { Byte.MIN_VALUE, Byte.MAX_VALUE, (byte) 0xff });
    }
}