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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import io.vertx.core.buffer.Buffer;

public final class ClickHouseBuffer {
    private static final int U_INT8_MAX = (1 << 8) - 1;
    private static final int U_INT16_MAX = (1 << 16) - 1;
    private static final long U_INT32_MAX = (1L << 32) - 1;
    private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);

    private static final long DATETIME_MAX = U_INT32_MAX * 1000L;

    private static final String EMPTY_STRING = "";

    protected final Buffer buffer;
    protected final TimeZone timeZone;

    public static ClickHouseBuffer wrap(Buffer buffer, TimeZone timeZone) {
        return new ClickHouseBuffer(buffer, timeZone);
    }

    public static ClickHouseBuffer wrap(Buffer buffer) {
        return wrap(buffer, null);
    }

    public static ClickHouseBuffer newInstance(TimeZone timeZone, int initialSizeHint) {
        return new ClickHouseBuffer(Buffer.buffer(initialSizeHint), timeZone);
    }

    public static ClickHouseBuffer newInstance(int initialSizeHint) {
        return newInstance(null, initialSizeHint);
    }

    public static Buffer asBuffer(String str) {
        return newInstance(str.length() * 2).writeString(str).buffer;
    }

    static void checkArgument(int value, int minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    static void checkArgument(long value, long minValue) {
        if (value < minValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should NOT less than ").append(minValue).toString());
        }
    }

    static void checkArgument(int value, int minValue, int maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    static void checkArgument(long value, long minValue, long maxValue) {
        if (value < minValue || value > maxValue) {
            throw new IllegalArgumentException(new StringBuilder().append("Given value(").append(value)
                    .append(") should between ").append(minValue).append(" and ").append(maxValue).toString());
        }
    }

    static List<String> splitByChar(String str, char delimiter) {
        return splitByChar(str, delimiter, true);
    }

    static List<String> splitByChar(String str, char delimiter, boolean tokenize) {
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

    private ClickHouseBuffer(Buffer buffer, TimeZone timeZone) {
        this.buffer = buffer != null ? buffer : Buffer.buffer();
        this.timeZone = timeZone != null ? timeZone : TimeZone.getDefault();
    }

    public ClickHouseBuffer writeUnsignedLeb128(int value) {
        checkArgument(value, 0);

        int remaining = value >>> 7;
        while (remaining != 0) {
            this.buffer.appendByte((byte) ((value & 0x7f) | 0x80));
            value = remaining;
            remaining >>>= 7;
        }
        this.buffer.appendByte((byte) (value & 0x7f));

        return this;
    }

    public ClickHouseBuffer writeByte(byte value) {
        this.buffer.appendByte(value);

        return this;
    }

    public ClickHouseBuffer writeBytes(byte[] value) {
        this.buffer.appendBytes(value);

        return this;
    }

    public ClickHouseBuffer writeBoolean(boolean value) {
        return writeByte(value ? (byte) 1 : (byte) 0);
    }

    public ClickHouseBuffer writeNull() {
        return writeBoolean(true);
    }

    public ClickHouseBuffer writeNonNull() {
        return writeBoolean(false);
    }

    public ClickHouseBuffer writeInt8(byte value) {
        return writeByte(value);
    }

    public ClickHouseBuffer writeInt8(int value) {
        checkArgument(value, Byte.MIN_VALUE);

        return value > Byte.MAX_VALUE ? writeUInt8(value) : writeByte((byte) value);
    }

    public ClickHouseBuffer writeUInt8(int value) {
        checkArgument(value, 0, U_INT8_MAX);

        return writeByte((byte) (value & 0xFFL));
    }

    public ClickHouseBuffer writeInt16(short value) {
        this.buffer.appendByte((byte) (0xFFL & value)).appendByte((byte) (0xFFL & (value >> 8)));
        return this;
    }

    public ClickHouseBuffer writeInt16(int value) {
        checkArgument(value, Short.MIN_VALUE);

        return value > U_INT16_MAX ? writeUInt16(value) : writeInt16((short) value);
    }

    public ClickHouseBuffer writeUInt16(int value) {
        checkArgument(value, 0, U_INT16_MAX);

        return writeInt16((short) (value & 0xFFFFL));
    }

    public ClickHouseBuffer writeInt32(int value) {
        this.buffer.appendByte((byte) (0xFFL & value)).appendByte((byte) (0xFFL & (value >> 8)))
                .appendByte((byte) (0xFFL & (value >> 16))).appendByte((byte) (0xFFL & (value >> 24)));
        return this;
    }

    public ClickHouseBuffer writeUInt32(long value) {
        checkArgument(value, 0, U_INT32_MAX);

        return writeInt32((int) (value & 0xFFFFFFFFL));
    }

    public ClickHouseBuffer writeInt64(long value) {
        value = Long.reverseBytes(value);

        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (value & 0xFFL);
            value >>= 8;
        }

        return writeBytes(bytes);
    }

    public ClickHouseBuffer writeUInt64(long value) {
        checkArgument(value, 0);

        return writeInt64(value);
    }

    public ClickHouseBuffer writeFloat32(float value) {
        return writeInt32(Float.floatToIntBits(value));
    }

    public ClickHouseBuffer writeFloat64(double value) {
        return writeInt64(Double.doubleToLongBits(value));
    }

    public ClickHouseBuffer writeBigInteger(BigInteger value) {
        byte[] bytes = value.toByteArray();
        for (int i = bytes.length; i > 0; i--) {
            writeByte(bytes[i - 1]);
        }

        writeBytes(new byte[16 - bytes.length]);

        return this;
    }

    private BigInteger toBigInteger(BigDecimal value, int scale) {
        return value.multiply(BigDecimal.valueOf(10).pow(scale)).toBigInteger();
    }

    public ClickHouseBuffer writeDecimal(BigDecimal value, int precision, int scale) {
        return precision > 18 ? writeDecimal128(value, scale)
                : (precision > 9 ? writeDecimal64(value, scale) : writeDecimal32(value, scale));
    }

    public ClickHouseBuffer writeDecimal32(BigDecimal value, int scale) {
        return writeInt32(toBigInteger(value, scale).intValue());
    }

    public ClickHouseBuffer writeDecimal64(BigDecimal value, int scale) {
        return writeInt64(toBigInteger(value, scale).longValue());
    }

    public ClickHouseBuffer writeDecimal128(BigDecimal value, int scale) {
        byte[] bytes = toBigInteger(value, scale).toByteArray();

        for (int i = bytes.length; i > 0; i--) {
            writeByte(bytes[i - 1]);
        }

        writeBytes(new byte[16 - bytes.length]);

        return this;
    }

    public ClickHouseBuffer writeDateTime(Date value) {
        Objects.requireNonNull(value);

        long time = value.getTime();
        if (time < 0L) { // 1970-01-01 00:00:00
            time = 0L;
        } else if (time > DATETIME_MAX) { // 2106-02-07 06:28:15
            time = DATETIME_MAX;
        }

        time = time / 1000L;

        if (time > Integer.MAX_VALUE) {
            // https://github.com/google/guava/blob/master/guava/src/com/google/common/io/LittleEndianDataOutputStream.java#L130
            this.buffer.appendBytes(new byte[] { (byte) (0x0FFL & time), (byte) (0x0FFL & (time >> 8)),
                    (byte) (0x0FFL & (time >> 16)), (byte) (0x0FFL & (time >> 24)) });
        } else {
            writeUInt32((int) time);
        }

        return this;
    }

    public ClickHouseBuffer writeDate(Date value) {
        Objects.requireNonNull(value);

        long localMillis = value.getTime() + timeZone.getOffset(value.getTime());
        int daysSinceEpoch = (int) (localMillis / MILLIS_IN_DAY);

        return writeUInt16(daysSinceEpoch);
    }

    public ClickHouseBuffer writeString(String value) {
        Objects.requireNonNull(value);

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        return writeUnsignedLeb128(bytes.length).writeBytes(bytes);
    }

    public ClickHouseBuffer writeDefaultValue(ClickHouseColumnInfo column) {
        switch (column.getType()) {
            case Int8:
                writeInt8(0);
                break;
            case Int16:
                writeInt16(0);
                break;
            case Int32:
                writeInt32(0);
                break;
            case Int64:
                writeInt64(0);
                break;
            case UInt8:
                writeUInt8(0);
                break;
            case UInt16:
                writeUInt16(0);
                break;
            case UInt32:
                writeUInt32(0);
                break;
            case UInt64:
                writeUInt64(0);
                break;
            case Float32:
                writeFloat32(0.0F);
                break;
            case Float64:
                writeFloat64(0.0D);
                break;
            case DateTime:
                writeUInt32(1);
                break;
            case Date:
                writeUInt16(1);
                break;
            case Decimal:
                writeDecimal(BigDecimal.ZERO, column.getPrecision(), column.getScale());
                break;
            case Decimal32:
                writeDecimal32(BigDecimal.ZERO, column.getScale());
                break;
            case Decimal64:
                writeDecimal64(BigDecimal.ZERO, column.getScale());
                break;
            case Decimal128:
                writeDecimal128(BigDecimal.ZERO, column.getScale());
                break;
            case String:
            default:
                writeString(EMPTY_STRING);
                break;
        }

        return this;
    }

    public Buffer unwrap() {
        return this.buffer;
    }
}