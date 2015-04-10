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

import java.io.IOException;

import com.cloudera.recordservice.thrift.TNetworkAddress;
import com.cloudera.recordservice.thrift.TPlanRequestResult;
import com.cloudera.recordservice.thrift.TRecordServiceException;
import com.cloudera.recordservice.thrift.TTask;

public class WorkerClientUtil {

  /**
   * Utility function to launch task 'taskId' in plan at the first host.
   * Local hosts are preferred over remote hosts.
   *
   * TODO: move this into a test/sample directory? Real frameworks have
   * to deal with much more stuff.
   */
  public static Records execTask(TPlanRequestResult plan, int taskId)
      throws TRecordServiceException, IOException {
    if (taskId >= plan.tasks.size() || taskId < 0) {
      throw new RuntimeException("Invalid task id.");
    }

    TTask task = plan.tasks.get(taskId);
    TNetworkAddress host = null;
    if (task.local_hosts.isEmpty()) {
      if (plan.hosts.isEmpty()) {
        throw new RuntimeException("No hosts are provided to run this task.");
      }
      host = plan.hosts.get(0);
    } else {
      host = task.local_hosts.get(0);
    }

    RecordServiceWorkerClient client = new RecordServiceWorkerClient();
    client.connect(host.hostname, host.port);
    return client.execAndFetch(task.task);
  }
}
