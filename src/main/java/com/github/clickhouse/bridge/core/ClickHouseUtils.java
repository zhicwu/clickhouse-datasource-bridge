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

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import io.vertx.core.json.JsonObject;

public final class ClickHouseUtils {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClickHouseUtils.class);

    public static final String VARIABLE_PREFIX = "{{";
    public static final String VARIABLE_SUFFIX = "}}";

    private static final String MSG_DIGEST_ALGORTITHM = "SHA-512";

    public static final String EMPTY_STRING = "";

    public static final int U_INT8_MAX = (1 << 8) - 1;
    public static final int U_INT16_MAX = (1 << 16) - 1;
    public static final long U_INT32_MAX = (1L << 32) - 1;
    public static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    public static final long DATETIME_MAX = U_INT32_MAX * 1000L;

    public static void checkArgument(int value, int minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    public static void checkArgument(long value, long minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    public static void checkArgument(int value, int minValue, int maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static void checkArgument(long value, long minValue, long maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    public static List<String> splitByChar(String str, char delimiter) {
        return splitByChar(str, delimiter, true);
    }

    public static List<String> splitByChar(String str, char delimiter, boolean tokenize) {
        LinkedList<String> list = new LinkedList<>();

        if (str != null) {
            int startIndex = 0;

            for (int i = 0, length = str.length(); i <= length; i++) {
                if (i == length || str.charAt(i) == delimiter) {
                    if (tokenize && i >= startIndex) {
                        String matched = str.substring(startIndex, i).trim();
                        if (matched.length() > 0) {
                            list.add(matched);
                        }
                    } else {
                        list.add(str.substring(startIndex, i));
                    }

                    startIndex = Math.min(i + 1, length);
                }
            }
        }

        return list;
    }

    public static String digest(JsonObject o) {
        return digest(o == null ? (String) null : o.encode());
    }

    public static String digest(String s) {
        if (s == null || s.length() == 0) {
            return EMPTY_STRING;
        }

        try {
            MessageDigest md = MessageDigest.getInstance(MSG_DIGEST_ALGORTITHM);
            return new BigInteger(1, md.digest(s.getBytes())).toString(16);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static void addTypedParameter(Map<String, TypedParameter<?>> map, TypedParameter<?> p) {
        if (map != null && p != null) {
            map.put(p.getName(), p);
        }
    }

    public static boolean fileExists(String file) {
        return Files.exists(Paths.get(file));
    }

    public static String loadTextFromFile(String file) {
        log.info("Loading text from file [{}]...", file);

        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(file), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append('\n'));
        } catch (Exception e) {
            log.warn("Failed to load text from file", e);
        }

        return contentBuilder.toString();
    }

    public static JsonObject loadJsonFromFile(String file) {
        log.info("Loading JSON from file [{}]...", file);

        JsonObject config = null;

        StringBuilder contentBuilder = new StringBuilder();
        try (Stream<String> stream = Files.lines(Paths.get(file), StandardCharsets.UTF_8)) {
            stream.forEach(s -> contentBuilder.append(s).append('\n'));
            config = new JsonObject(contentBuilder.toString());
        } catch (Exception e) {
            log.warn("Failed to load JSON from file", e);
        }

        return config == null ? new JsonObject() : config;
    }

    public static String applyVariables(String template, UnaryOperator<String> operator) {
        if (template == null) {
            template = EMPTY_STRING;
        }

        if (operator == null) {
            return template;
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0, len = template.length(); i < len; i++) {
            int index = template.indexOf(VARIABLE_PREFIX, i);
            if (index != -1) {
                sb.append(template.substring(i, index));

                i = index;
                index = template.indexOf(VARIABLE_SUFFIX, i);

                if (index != -1) {
                    String variable = template.substring(i + VARIABLE_PREFIX.length(), index).trim();
                    String value = operator.apply(variable);
                    if (value == null) {
                        i += VARIABLE_PREFIX.length() - 1;
                        sb.append(VARIABLE_PREFIX);
                    } else {
                        i = index + VARIABLE_SUFFIX.length() - 1;
                        sb.append(value);
                    }
                } else {
                    sb.append(template.substring(i));
                    break;
                }
            } else {
                sb.append(template.substring(i));
                break;
            }
        }

        return sb.toString();
    }

    public static String applyVariables(String template, Map<String, String> variables) {
        return applyVariables(template, variables == null || variables.size() == 0 ? null : variables::get);
    }

    private ClickHouseUtils() {
    }
}