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

import com.cloudera.recordservice.thrift.TPlanRequestParams;
import com.cloudera.recordservice.thrift.TRequestType;

// Abstraction over request types.
public class Request {

  // Creates a request that is a SQL query.
  public static Request createSqlRequest(String query) {
    TPlanRequestParams request = new TPlanRequestParams();
    request.request_type = TRequestType.Sql;
    request.sql_stmt = query;
    return new Request(request);
  }

  protected TPlanRequestParams request_;

  private Request(TPlanRequestParams request) {
    request_ = request;
  }
}
