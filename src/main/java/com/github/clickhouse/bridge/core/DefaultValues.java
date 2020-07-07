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

import static com.github.clickhouse.bridge.core.ClickHouseUtils.EMPTY_STRING;

import java.math.BigDecimal;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import io.vertx.core.json.JsonObject;

public class DefaultValues {
        // Signed
        public final TypedParameter<Integer> int8;
        public final TypedParameter<Integer> int16;
        public final TypedParameter<Integer> int32;
        public final TypedParameter<Long> int64;

        // Unsigned
        public final TypedParameter<Integer> uint8;
        public final TypedParameter<Integer> uint16;
        public final TypedParameter<Long> uint32;
        public final TypedParameter<Long> uint64;

        // Floating point
        public final TypedParameter<Float> float32;
        public final TypedParameter<Double> float64;

        // Date time
        public final TypedParameter<Integer> date;
        public final TypedParameter<Long> datetime;
        public final TypedParameter<Long> datetime64;

        // Decimals
        public final TypedParameter<BigDecimal> decimal;
        public final TypedParameter<BigDecimal> decimal32;
        public final TypedParameter<BigDecimal> decimal64;
        public final TypedParameter<BigDecimal> decimal128;

        // Misc
        public final TypedParameter<String> string;

        private final Map<String, TypedParameter<?>> types = new TreeMap<>();

        public DefaultValues() {
                ClickHouseUtils.addTypedParameter(types,
                                this.int8 = new TypedParameter<>(Integer.class, ClickHouseDataType.Int8.name(), 0));
                ClickHouseUtils.addTypedParameter(types,
                                this.int16 = new TypedParameter<>(Integer.class, ClickHouseDataType.Int16.name(), 0));
                ClickHouseUtils.addTypedParameter(types,
                                this.int32 = new TypedParameter<>(Integer.class, ClickHouseDataType.Int32.name(), 0));
                ClickHouseUtils.addTypedParameter(types,
                                this.int64 = new TypedParameter<>(Long.class, ClickHouseDataType.Int64.name(), 0L));

                ClickHouseUtils.addTypedParameter(types,
                                this.uint8 = new TypedParameter<>(Integer.class, ClickHouseDataType.UInt8.name(), 0));
                ClickHouseUtils.addTypedParameter(types,
                                this.uint16 = new TypedParameter<>(Integer.class, ClickHouseDataType.UInt16.name(), 0));
                ClickHouseUtils.addTypedParameter(types,
                                this.uint32 = new TypedParameter<>(Long.class, ClickHouseDataType.UInt32.name(), 0L));
                ClickHouseUtils.addTypedParameter(types,
                                this.uint64 = new TypedParameter<>(Long.class, ClickHouseDataType.UInt64.name(), 0L));

                ClickHouseUtils.addTypedParameter(types, this.float32 = new TypedParameter<>(Float.class,
                                ClickHouseDataType.Float32.name(), 0.0F));
                ClickHouseUtils.addTypedParameter(types, this.float64 = new TypedParameter<>(Double.class,
                                ClickHouseDataType.Float64.name(), 0.0));

                ClickHouseUtils.addTypedParameter(types,
                                this.date = new TypedParameter<>(Integer.class, ClickHouseDataType.Date.name(), 1));
                ClickHouseUtils.addTypedParameter(types, this.datetime = new TypedParameter<>(Long.class,
                                ClickHouseDataType.DateTime.name(), 1L));
                ClickHouseUtils.addTypedParameter(types, this.datetime64 = new TypedParameter<>(Long.class,
                                ClickHouseDataType.DateTime64.name(), 1000L));

                ClickHouseUtils.addTypedParameter(types, this.decimal = new TypedParameter<>(BigDecimal.class,
                                ClickHouseDataType.Decimal.name(), BigDecimal.ZERO));
                ClickHouseUtils.addTypedParameter(types, this.decimal32 = new TypedParameter<>(BigDecimal.class,
                                ClickHouseDataType.Decimal32.name(), BigDecimal.ZERO));
                ClickHouseUtils.addTypedParameter(types, this.decimal64 = new TypedParameter<>(BigDecimal.class,
                                ClickHouseDataType.Decimal64.name(), BigDecimal.ZERO));
                ClickHouseUtils.addTypedParameter(types, this.decimal128 = new TypedParameter<>(BigDecimal.class,
                                ClickHouseDataType.Decimal128.name(), BigDecimal.ZERO));

                ClickHouseUtils.addTypedParameter(types, this.string = new TypedParameter<>(String.class,
                                ClickHouseDataType.String.name(), EMPTY_STRING));
        }

        public DefaultValues(JsonObject... params) {
                this();

                for (JsonObject p : params) {
                        merge(p);
                }
        }

        public DefaultValues merge(JsonObject p) {
                if (p != null) {
                        for (Entry<String, Object> entry : p) {
                                String name = entry.getKey();
                                TypedParameter<?> tp = this.types.get(name);
                                if (tp != null) {
                                        tp.merge(p);
                                }
                        }
                }

                return this;
        }

        public TypedParameter<?> getTypedValue(ClickHouseDataType type) {
                return this.types.get(type.name());
        }
}