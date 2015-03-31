// Copyright 2012 Cloudera Inc.
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

#ifndef RS_COMMON_H
#define RS_COMMON_H

#include <stdio.h>
#include <exception>
#include <sstream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"

// TODO: remove this file. We only need this because we link against the impala
// generated thrift lib.

// Comparator for THostPorts. Thrift declares this (in gen-cpp/Types_types.h) but
// never defines it.
bool impala::TNetworkAddress::operator<(const impala::TNetworkAddress& that) const {
  // TODO: do we need to compare hdfs_host_name?
  if (this->hostname < that.hostname) {
    return true;
  } else if ((this->hostname == that.hostname) && (this->port < that.port)) {
    return true;
  }
  return false;
}

bool impala::TAccessEvent::operator<(const impala::TAccessEvent& that) const {
  return this->name < that.name;
}

#endif
