// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

// #include <glog/logging.h>
#include <set>
#include <string>
#include "pink/include/pink_conn.h"
#include "binlog_receiver_thread.h"
#include "master_conn.h"
#include "pika_port.h"
#include "pika_command.h"
#include "conf.h"

extern PikaPort* g_pika_port;

  BinlogReceiverThread::BinlogReceiverThread(std::string host, int port,
          int cron_interval):
          conn_factory_(this), handles_(this) {

    std::set<std::string> ipset;
    ipset.insert(host);
    thread_rep_ = pink::NewHolyThread(ipset, port, &conn_factory_, false,
                                    cron_interval, &handles_);

  // to prevent HolyThread::DoCronTask close the pika sender connection
   thread_rep_->set_keepalive_timeout(0);

}

BinlogReceiverThread::~BinlogReceiverThread() {
  thread_rep_->StopThread();
  pinfo("BinlogReceiver thread %llu exit!!!", thread_rep_->thread_id());
  delete thread_rep_;
}

int BinlogReceiverThread::StartThread() {
  return thread_rep_->StartThread();
}

bool BinlogReceiverThread::Handles::AccessHandle(std::string& ip) const {
  if (ip == "127.0.0.1") {
    ip = g_conf.local_ip;
  }
  pinfo("Master Binlog Sender: %s connecting", ip.c_str());
  // if (binlog_receiver_->thread_rep_->conn_num() != 0 ||
  //     !g_pika_port->ShouldAccessConnAsMaster(ip)) {
  //   pinfo("BinlogReceiverThread AccessHandle failed";
  //   return false;
  // }
  g_pika_port->PlusMasterConnection();
  return true;
}

void BinlogReceiverThread::KillBinlogSender() {
  thread_rep_->KillAllConns();
}
