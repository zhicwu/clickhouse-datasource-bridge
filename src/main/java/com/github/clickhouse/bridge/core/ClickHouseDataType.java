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

// https://clickhouse-docs.readthedocs.io/en/latest/data_types/
public enum ClickHouseDataType {
    // Signed
    Int8, Int16, Int32, Int64,

    // Unsigned
    UInt8, UInt16, UInt32, UInt64,

    // Floating point
    Float32, Float64,

    // Date time
    Date, DateTime, DateTime64,

    // Decimals
    Decimal, Decimal32, Decimal64, Decimal128,

    // Misc
    String;

    public static final int DEFAULT_DECIMAL_PRECISON = 10;
    public static final int DEFAULT_DECIMAL_SCALE = 4;
    public static final int DEFAULT_DECIMAL32_PRECISON = 9;
    public static final int DEFAULT_DECIMAL32_SCALE = 2;
    public static final int DEFAULT_DECIMAL64_PRECISON = 18;
    public static final int DEFAULT_DECIMAL64_SCALE = 4;
    public static final int DEFAULT_DECIMAL128_PRECISON = 38;
    public static final int DEFAULT_DECIMAL128_SCALE = 8;

    // https://clickhouse.tech/docs/en/sql-reference/data-types/decimal/
    public static final int MAX_PRECISON = 38;

    public static final boolean DEFAULT_NULLABLE = true;
    public static final int DEFAULT_PRECISION = 0;
    public static final int DEFAULT_SCALE = 0;

    public static final int MAX_DATETIME64_SCALE = 18;
    // Tick size (precision): 10-precision seconds
    public static final int DEFAULT_DATETIME64_SCALE = 3;
}
