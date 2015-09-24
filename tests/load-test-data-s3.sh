#!/usr/bin/env bash
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
#
# Loads the test tables for S3.
# TODO: reconcile this with load-test-data.

# Exit on reference to uninitialized variables and non-zero exit codes
set -u
set -e

# Load the test data we need.
cd $RECORD_SERVICE_HOME
impala-shell.sh -f tests/create-tbls-s3.sql

