// Confidential Cloudera Information: Covered by NDA.
// Copyright 2015 Cloudera Inc.
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

package com.cloudera.recordservice.mr.security;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.token.Token;

import com.cloudera.recordservice.thrift.TDelegationToken;

/**
 * Utilities to handle token serialization/deserialization.
 */
public class TokenUtils {
  /**
   * Serializes a token to TDelegationToken.
   */
  public static TDelegationToken
      toTDelegationToken(Token<DelegationTokenIdentifier> t) throws IOException {
    TDelegationToken token = new TDelegationToken();
    token.identifier = encodeAsString(t.getIdentifier());
    token.password = encodeAsString(t.getPassword());
    token.token = ByteBuffer.wrap(t.encodeToUrlString().getBytes());
    return token;
  }

  /**
   * Deserializes a token from TDelegationToken
   */
  public static Token<DelegationTokenIdentifier>
      fromTDelegationToken(TDelegationToken t) throws IOException {
    Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
    token.decodeFromUrlString(new String(t.getToken()));
    return token;
  }

  /**
   * Encodes the byte array as a string.
   */
  public static String encodeAsString(byte[] v) {
    return new String(Base64.encodeBase64(v));
  }
}
