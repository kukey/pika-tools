// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef MASTER_CONN_H_
#define MASTER_CONN_H_

/*
 * **************Header**************
 * | <Transfer Type> | <Body Lenth> |
 *       2 Bytes         4 Bytes
 */
#define HEADER_LEN 6

#include "pink/include/pink_conn.h"
#include "pink/include/redis_parser.h"
#include "pika_command.h"

#include "binlog_transverter.h"
#include "pika_binlog_parser.h"
class BinlogReceiverThread;

enum PortTransferOperate{
  kTypePortAuth = 1,
  kTypePortBinlog = 2
};

class MasterConn: public pink::PinkConn {
 public:
  MasterConn(int fd, std::string ip_port, void* worker_specific_data);
  virtual ~MasterConn();

  virtual pink::ReadStatus GetRequest();
  virtual pink::WriteStatus SendReply();


  bool ProcessAuth(const pink::RedisCmdArgsType& argv);
  bool ProcessBinlogData(const pink::RedisCmdArgsType& argv, const PortBinlogItem& binlog_item);


  BinlogHeader binlog_header_;
  PortBinlogItem binlog_item_;




 private:
    static int DealMessage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv);
    pink::ReadStatus ParseRedisParserStatus(pink::RedisParserStatus status);



    char* rbuf_;
    int rbuf_len_;
    int msg_peak_;

    bool is_authed_;

    // For Redis Protocol parser
    int last_read_pos_;
    long bulk_len_;

    pink::RedisParser redis_parser_;
    PikaBinlogParser binlog_parser_;



    BinlogReceiverThread* binlog_receiver_;
};

#endif
