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
import java.util.Objects;
import java.util.TimeZone;

import io.vertx.core.buffer.Buffer;

import static com.github.clickhouse.bridge.core.ClickHouseUtils.*;

public final class ClickHouseBuffer {
    protected final Buffer buffer;
    protected final TimeZone timezone;

    public static ClickHouseBuffer wrap(Buffer buffer, TimeZone timezone) {
        return new ClickHouseBuffer(buffer, timezone);
    }

    public static ClickHouseBuffer wrap(Buffer buffer) {
        return wrap(buffer, null);
    }

    public static ClickHouseBuffer newInstance(int initialSizeHint, TimeZone timezone) {
        return new ClickHouseBuffer(Buffer.buffer(initialSizeHint), timezone);
    }

    public static ClickHouseBuffer newInstance(int initialSizeHint) {
        return newInstance(initialSizeHint, null);
    }

    public static Buffer asBuffer(String str) {
        return newInstance(str.length() * 2).writeString(str).buffer;
    }

    private ClickHouseBuffer(Buffer buffer, TimeZone timezone) {
        this.buffer = buffer != null ? buffer : Buffer.buffer();
        this.timezone = timezone;
    }

    public ClickHouseBuffer writeUnsignedLeb128(int value) {
        ClickHouseUtils.checkArgument(value, 0);

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
        ClickHouseUtils.checkArgument(value, Byte.MIN_VALUE);

        return value > Byte.MAX_VALUE ? writeUInt8(value) : writeByte((byte) value);
    }

    public ClickHouseBuffer writeUInt8(int value) {
        ClickHouseUtils.checkArgument(value, 0, U_INT8_MAX);

        return writeByte((byte) (value & 0xFFL));
    }

    public ClickHouseBuffer writeInt16(short value) {
        this.buffer.appendByte((byte) (0xFFL & value)).appendByte((byte) (0xFFL & (value >> 8)));
        return this;
    }

    public ClickHouseBuffer writeInt16(int value) {
        ClickHouseUtils.checkArgument(value, Short.MIN_VALUE);

        return value > U_INT16_MAX ? writeUInt16(value) : writeInt16((short) value);
    }

    public ClickHouseBuffer writeUInt16(int value) {
        ClickHouseUtils.checkArgument(value, 0, U_INT16_MAX);

        return writeInt16((short) (value & 0xFFFFL));
    }

    public ClickHouseBuffer writeInt32(int value) {
        this.buffer.appendByte((byte) (0xFFL & value)).appendByte((byte) (0xFFL & (value >> 8)))
                .appendByte((byte) (0xFFL & (value >> 16))).appendByte((byte) (0xFFL & (value >> 24)));
        return this;
    }

    public ClickHouseBuffer writeUInt32(long value) {
        ClickHouseUtils.checkArgument(value, 0, U_INT32_MAX);

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
        ClickHouseUtils.checkArgument(value, 0);

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
        return writeDateTime(value, null);
    }

    public ClickHouseBuffer writeDateTime(Date value, TimeZone tz) {
        Objects.requireNonNull(value);

        return writeDateTime(value.getTime(), tz);
    }

    public ClickHouseBuffer writeDateTime(long time, TimeZone tz) {
        if ((tz = tz == null ? this.timezone : tz) != null) {
            time += tz.getOffset(time);
        }

        if (time <= 0L) { // 0000-00-00 00:00:00
            time = 1L;
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

    public ClickHouseBuffer writeDateTime64(Date value) {
        return writeDateTime64(value, null);
    }

    public ClickHouseBuffer writeDateTime64(Date value, TimeZone tz) {
        Objects.requireNonNull(value);

        return writeDateTime64(value.getTime(), tz);
    }

    // ClickHouse's DateTime64 supports precision from 0 to 18, but JDBC only
    // supports 3(millisecond)
    public ClickHouseBuffer writeDateTime64(long time, TimeZone tz) {
        if ((tz = tz == null ? this.timezone : tz) != null) {
            time += tz.getOffset(time);
        }

        if (time <= 0L) { // 0000-00-00 00:00:00.000
            time = 1L;
        }

        return this.writeUInt64(time);
    }

    public ClickHouseBuffer writeDate(Date value) {
        Objects.requireNonNull(value);

        TimeZone tz = this.timezone == null ? TimeZone.getDefault() : this.timezone;
        long localMillis = value.getTime() + tz.getOffset(value.getTime());
        int daysSinceEpoch = (int) (localMillis / MILLIS_IN_DAY);

        return writeUInt16(daysSinceEpoch);
    }

    public ClickHouseBuffer writeString(String value) {
        return writeString(value, false);
    }

    public ClickHouseBuffer writeString(String value, boolean normalize) {
        Objects.requireNonNull(value);

        byte[] bytes = (normalize ? value.replace('\r', ' ') : value).getBytes(StandardCharsets.UTF_8);
        return writeUnsignedLeb128(bytes.length).writeBytes(bytes);
    }

    public ClickHouseBuffer writeDefaultValue(ClickHouseColumnInfo column, DefaultValues defaultValues) {
        switch (column.getType()) {
            case Int8:
                writeInt8(defaultValues.int8.getValue());
                break;
            case Int16:
                writeInt16(defaultValues.int16.getValue());
                break;
            case Int32:
                writeInt32(defaultValues.int32.getValue());
                break;
            case Int64:
                writeInt64(defaultValues.int64.getValue());
                break;
            case UInt8:
                writeUInt8(defaultValues.uint8.getValue());
                break;
            case UInt16:
                writeUInt16(defaultValues.uint16.getValue());
                break;
            case UInt32:
                writeUInt32(defaultValues.uint32.getValue());
                break;
            case UInt64:
                writeUInt64(defaultValues.uint64.getValue());
                break;
            case Float32:
                writeFloat32(defaultValues.float32.getValue());
                break;
            case Float64:
                writeFloat64(defaultValues.float64.getValue());
                break;
            case DateTime:
                writeUInt32(defaultValues.datetime.getValue());
                break;
            case DateTime64:
                writeUInt64(defaultValues.datetime64.getValue());
                break;
            case Date:
                writeUInt16(defaultValues.date.getValue());
                break;
            case Decimal:
                writeDecimal(defaultValues.decimal.getValue(), column.getPrecision(), column.getScale());
                break;
            case Decimal32:
                writeDecimal32(defaultValues.decimal32.getValue(), column.getScale());
                break;
            case Decimal64:
                writeDecimal64(defaultValues.decimal64.getValue(), column.getScale());
                break;
            case Decimal128:
                writeDecimal128(defaultValues.decimal128.getValue(), column.getScale());
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