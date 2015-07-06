// Confidential Cloudera Information: Covered by NDA.
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

#include "external-mini-cluster.h"
#include <boost/lexical_cast.hpp>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>
#include <iostream>
#include <sstream>
#include <vector>
#include <stdio.h>

using namespace boost;
using namespace std;

namespace recordservice {

const int BASE_PORT = 30000;

const char* ExternalMiniCluster::Impalad::NAME = "impalad";
const char* ExternalMiniCluster::Statestored::NAME = "statestored";
const char* ExternalMiniCluster::Catalogd::NAME = "catalogd";

ExternalMiniCluster::ExternalMiniCluster(bool debug)
 : debug_(debug),
   impala_home_(getenv("IMPALA_HOME")),
   statestored_(NULL),
   catalogd_(NULL),
   next_port_(BASE_PORT) {
  if (impala_home_ == NULL) {
    cerr << "Must set IMPALA_HOME" << endl;
    exit(1);
  }
  build_home_ = impala_home_;
  build_home_ += string("/be/build/") + (debug_ ? "debug/" : "release/");
}

ExternalMiniCluster::~ExternalMiniCluster() {
  if (statestored_ != NULL) Kill(statestored_);
  if (catalogd_ != NULL) Kill(catalogd_);
  unordered_set<Impalad*> copy = impalads_;
  impalads_.clear();
  for (unordered_set<Impalad*>::iterator it = copy.begin(); it != copy.end(); ++it) {
    Kill(*it);
  }
}

string ExternalMiniCluster::Statestored::GetBinaryPath() {
  return "statestore/statestored";
}

string ExternalMiniCluster::Catalogd::GetBinaryPath() {
  return "catalog/catalogd";
}

string ExternalMiniCluster::Impalad::GetBinaryPath() {
  return "service/impalad";
}

string ExternalMiniCluster::NextPort() {
  return lexical_cast<string>(next_port_++);
}

bool ExternalMiniCluster::Process::Start() {
  return subprocess_.Start();
}

vector<string> ConstructArgs(const string& binary, const map<string, string>& args) {
  vector<string> ret;
  ret.push_back(binary);

  for (map<string, string>::const_iterator it = args.begin(); it != args.end(); ++it) {
    ret.push_back(string("--") + it->first + "=" + it->second);
  }
  return ret;
}

// Starts a catalogd process.
bool ExternalMiniCluster::StartCatalogd(Catalogd** process) {
  *process = NULL;
  if (catalogd_ != NULL) {
    cerr << "Cannot start more than one catalogd." << endl;
    return false;
  }

  string binary = build_home_ + Catalogd::GetBinaryPath();
  // TODO: populate these args?
  map<string, string> args;

  *process = new Catalogd(binary, ConstructArgs(binary, args));
  if (!(*process)->Start()) return false;
  catalogd_ = *process;
  cout << "Started " << (*process)->name() << " pid: " << (*process)->pid() << endl;
  return true;
}

// Starts a statestored process
bool ExternalMiniCluster::StartStatestored(Statestored** process) {
  *process = NULL;
  if (statestored_ != NULL) {
    cerr << "Cannot start more than one statestored." << endl;
    return false;
  }

  string binary = build_home_ + Statestored::GetBinaryPath();
  // TODO: populate these args?
  map<string, string> args;
  *process = new Statestored(binary, ConstructArgs(binary, args));
  if (!(*process)->Start()) return false;
  statestored_ = *process;
  cout << "Started " << (*process)->name() << " pid: " << (*process)->pid() << endl;
  return true;
}

// Starts an impalad, optionally running the recordservice planner and worker
// services.
bool ExternalMiniCluster::StartImpalad(
    bool start_record_service_planner, bool start_record_service_worker,
    Impalad** process) {
  *process = NULL;

  string binary = build_home_ + Impalad::GetBinaryPath();
  map<string, string> args;

  args["beeswax_port"] = NextPort();
  args["hs2_port"] = NextPort();
  args["be_port"] = NextPort();
  args["webserver_port"] = NextPort();
  args["state_store_subscriber_port"] = NextPort();

  if (start_record_service_planner) {
    args["recordservice_planner_port"] = NextPort();
  } else {
    args["recordservice_planner_port"] = "0";
  }
  if (start_record_service_worker) {
    args["recordservice_worker_port"] = NextPort();
  } else {
    args["recordservice_worker_port"] = "0";
  }

  *process = new Impalad(binary, ConstructArgs(binary, args));
  if (!(*process)->Start()) return false;

  if (start_record_service_planner) {
    (*process)->planner_port_ = atoi(args["recordservice_planner_port"].c_str());
  }
  impalads_.insert(*process);
  cout << "Started " << (*process)->name() << " pid: " << (*process)->pid() << endl
       << "    Debug webpage running on port: " << args["webserver_port"] << endl;
  return true;
}

bool ExternalMiniCluster::Process::Wait(int* ret) {
  return subprocess_.Wait(ret);
}

bool ExternalMiniCluster::Kill(Process* process) {
  cout << "Killing " << process->name() << endl;
  bool ret = process->subprocess_.Kill(SIGKILL);
  if (!ret) {
    cerr << "Could not kill process. ret=" << ret << endl;;
    return false;
  }
  int wait_ret;
  process->Wait(&wait_ret);
  if (process == statestored_) {
    statestored_ = NULL;
  } else if (process == catalogd_) {
    catalogd_ = NULL;
  } else {
    impalads_.erase((Impalad*)process);
  }
  delete process;
  return true;
}

}

