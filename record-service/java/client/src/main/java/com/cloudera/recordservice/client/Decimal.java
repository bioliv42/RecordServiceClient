// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.recordservice.client;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

// Representation of fixed precision and scale decimal values. This object
// is intended to be reused.
public class Decimal {
  public static final int MAX_PRECISION = 38;

  // 2's complement little endian.
  private byte[] bytes_;
  private int precision_;
  private int scale_;

  public static int computeByteSize(int precision, int scale) {
    if (scale < 0 || precision < 0) {
      throw new IllegalArgumentException("Precision and scale must be non-negative.");
    }
    if (precision > MAX_PRECISION) {
      throw new IllegalArgumentException("Max supported precision is " + MAX_PRECISION);
    }
    if (scale > precision) {
      throw new IllegalArgumentException("Scale cannot be greater than precision.");
    }

    if (precision <= 9) {
      return 4;
    } else if (precision <= 18) {
      return 8;
    } else {
      return 16;
    }
  }

  public Decimal(int precision, int scale) {
    bytes_ = new byte[computeByteSize(precision, scale)];
    precision_ = precision;
    scale_ = scale;
  }

  public int getPrecision() { return precision_; }
  public int getScale() { return scale_; }
  public byte[] getBytes() { return bytes_; }

  /**
   * Returns the value as an unscaled BigInteger, that is the decimal
   * value is toBigInteger() / 10^scale.
   */
  public BigInteger toBigInteger() {
    return new BigInteger(bytes_);
  }

  /**
   * Returns the value as a BigDecimal.
   */
  public BigDecimal toBigDecimal() {
    return new BigDecimal(toBigInteger(), scale_);
  }

  protected void set(ByteBuffer buffer, int offset, int len) {
    // Copy and reverse the byte order.
    // TODO: we could defer this copy until toBigInteger().
    for (int i = 0; i < len; ++i) {
      bytes_[i] = buffer.get(offset + len - i - 1);
    }
  }
}
