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

import com.cloudera.recordservice.thrift.TProtocolVersion;

/**
 * Utility class to convert from thrift classes to client classes. We should
 * never be returning thrift classes as part of the client API.
 */
public class ThriftUtils {
  protected static ProtocolVersion fromThrift(TProtocolVersion v) {
    switch (v) {
    case V1: return ProtocolVersion.V1;
    default:
      // TODO: is this right for mismatched versions? Default to a lower
      // version?
      throw new RuntimeException("Unrecognized version: " + v);
    }
  }
}
