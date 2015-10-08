#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from optparse import OptionParser
import os
import time
import subprocess

parser = OptionParser()
parser.add_option("--host", dest="host", default="localhost",
    help="Host to connect to.")
(options, args) = parser.parse_args()
IMPALA_SHELL = os.environ['IMPALA_HOME'] + "/bin/impala-shell.sh"

TEST_USER = "sentry_test_user"
TEST_ROLE = "SENTRY_TEST_ROLE"

# A dummy class used to store test output
class TestOutput:
  msg = ""

# Runs 'cmd' and waits for it to finish. Add stdout to 'output'
# if it is not None. Also add stderr if 'stderr' is True.
def run_shell_cmd(cmd, output = None, stderr = False):
  tmp_file = "/tmp/sentry_test_result"
  cmd += " > " + tmp_file
  if stderr: cmd += " 2>&1"
  ret = subprocess.call(cmd, shell = True)
  if output:
    with open(tmp_file, 'r') as f:
      for line in f:
        output.msg += line.strip() + '\n'

  if ret != 0:
    raise Exception("Failed to run cmd: '%s' ret=%s" % (cmd, ret))

def run_impala_query(query):
  return run_shell_cmd(IMPALA_SHELL + " -q \"" + query + "\"")

if __name__ == "__main__":
  print("Running against host: " + options.host)

  try:
    run_shell_cmd("sudo /usr/sbin/useradd " + TEST_USER)
  except Exception:
    pass

  run_impala_query(
    "DROP VIEW IF EXISTS tpch.nation_view;" +
    "CREATE VIEW tpch.nation_view AS SELECT n_name FROM tpch.nation")

  try:
    run_impala_query("CREATE ROLE " + TEST_ROLE)
  except Exception as e:
    pass

  run_impala_query(
    "GRANT ROLE " + TEST_ROLE + " TO GROUP " + TEST_USER + ";" +
    "GRANT SELECT ON TABLE tpch.nation_view TO ROLE " + TEST_ROLE + ";" +
    "GRANT ALL ON URI 'hdfs:/test-warehouse/tpch.nation' TO ROLE " + TEST_ROLE)

  # Need to wait for a while before the Sentry change is populated.
  # TODO: how to avoid this waiting?
  time_to_wait = 60
  print "Waiting for " + str(time_to_wait) + " seconds..."
  time.sleep(time_to_wait)

  try:
    test_output = TestOutput()
    os.chdir(os.environ['RECORD_SERVICE_HOME'] + '/java/core')
    run_shell_cmd(
      "mvn test -Duser.name=" + TEST_USER +
      " -Dtest=TestSentry", test_output, True)
  except Exception as e:
    print e.message
  finally:
    print test_output.msg
    run_shell_cmd("sudo /usr/sbin/userdel " + TEST_USER)
    run_impala_query("DROP ROLE " + TEST_ROLE)

