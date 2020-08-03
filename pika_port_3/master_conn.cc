// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.


#include <iostream>

#include "binlog_transverter.h"
#include "const.h"
#include "master_conn.h"
#include "binlog_receiver_thread.h"
#include "log.h"
#include "pika_port.h"

extern PikaPort *g_pika_port;

MasterConn::MasterConn(int fd, std::string ip_port, void* worker_specific_data)
    : PinkConn(fd, ip_port, NULL),
      rbuf_(nullptr),
      rbuf_len_(0),
      msg_peak_(0),
      is_authed_(false),
      last_read_pos_(-1),
      bulk_len_(-1) {
    binlog_receiver_ = reinterpret_cast<BinlogReceiverThread*>(worker_specific_data);
    pink::RedisParserSettings settings;
    settings.DealMessage = &(MasterConn::DealMessage);
    redis_parser_.RedisParserInit(REDIS_PARSER_REQUEST, settings);
    redis_parser_.data = this;
}

MasterConn::~MasterConn() {
  free(rbuf_);
}


pink::ReadStatus MasterConn::ParseRedisParserStatus(pink::RedisParserStatus status) {
    if (status == pink::kRedisParserInitDone) {
        return pink::kOk;
    } else if (status == pink::kRedisParserHalf) {
        return pink::kReadHalf;
    } else if (status == pink::kRedisParserDone) {
        return pink::kReadAll;
    } else if (status == pink::kRedisParserError) {
        pink::RedisParserError error_code = redis_parser_.get_error_code();
        switch (error_code) {
            case pink::kRedisParserOk :
                // status is error cant be ok
                return pink::kReadError;
            case pink::kRedisParserInitError :
                return pink::kReadError;
            case pink::kRedisParserFullError :
                return pink::kFullError;
            case pink::kRedisParserProtoError :
                return pink::kParseError;
            case pink::kRedisParserDealError :
                return pink::kDealError;
            default :
                return pink::kReadError;
        }
    } else {
        return pink::kReadError;
    }
}





pink::ReadStatus MasterConn::GetRequest() {
    ssize_t nread = 0;
    int next_read_pos = last_read_pos_ + 1;

    int remain = rbuf_len_ - next_read_pos;  // Remain buffer size
    int new_size = 0;
    if (remain == 0) {
        new_size = rbuf_len_ + REDIS_IOBUF_LEN;
        remain += REDIS_IOBUF_LEN;
    } else if (remain < bulk_len_) {
        new_size = next_read_pos + bulk_len_;
        remain = bulk_len_;
    }
    if (new_size > rbuf_len_) {
        if (new_size > REDIS_MAX_MESSAGE) {
            return pink::kFullError;
        }
        rbuf_ = static_cast<char*>(realloc(rbuf_, new_size));
        if (rbuf_ == nullptr) {
            return pink::kFullError;
        }
        rbuf_len_ = new_size;
    }

    nread = read(fd(), rbuf_ + next_read_pos, remain);
    if (nread == -1) {
        if (errno == EAGAIN) {
            nread = 0;
            return pink::kReadHalf; // HALF
        } else {
            // error happened, close client
            return pink::kReadError;
        }
    } else if (nread == 0) {
        // client closed, close client
        return pink::kReadClose;
    }

    // assert(nread > 0);
    last_read_pos_ += nread;
    msg_peak_ = last_read_pos_;

    char* cur_process_p = rbuf_ + next_read_pos;
    int processed_len = 0;
    pink::ReadStatus read_status = pink::kReadError;
    while (processed_len < nread) {
        int remain = nread - processed_len;
        int scrubed_len = 0; // len binlog parser may scrub
        char* cur_scrub_start = cur_process_p + processed_len;
        int cur_bp_processed_len = 0; // current binlog parser processed len
        pink::ReadStatus scrub_status = binlog_parser_.ScrubReadBuffer(cur_scrub_start, remain, &cur_bp_processed_len, &scrubed_len, &binlog_header_, &binlog_item_);
        processed_len += cur_bp_processed_len;
        // return kReadAll or kReadHalf or kReadError
        if (scrub_status != pink::kReadAll) {
            read_status = scrub_status;
            break;
        }

        char* cur_redis_parser_start = cur_scrub_start + scrubed_len;
        int cur_parser_buff_len = cur_bp_processed_len - scrubed_len;
        int redis_parser_processed_len = 0; // parsed len already updated by cur_bp_processed_len useless here

        pink::RedisParserStatus ret = redis_parser_.ProcessInputBuffer(
                cur_redis_parser_start, cur_parser_buff_len, &redis_parser_processed_len);
        read_status = ParseRedisParserStatus(ret);
        if (read_status != pink::kReadAll) {
            break;
        }
    }
    if (read_status == pink::kReadAll || read_status == pink::kReadHalf) {
        last_read_pos_ = -1;
        bulk_len_ = redis_parser_.get_bulk_len();
    }
    return read_status;
}


pink::WriteStatus MasterConn::SendReply() {
  return pink::kWriteAll;
}


// -----------------
bool MasterConn::ProcessAuth(const pink::RedisCmdArgsType& argv) {
  if (argv.empty() || argv.size() != 2) {
    return false;
  }

  if (argv[0] == "auth") {
    if (argv[1] == std::to_string(g_pika_port->sid())) {
      is_authed_ = true;
      pinfo("BinlogReceiverThread AccessHandle succeeded, My server id: %d, Master auth server id:%s",
        g_pika_port->sid(), argv[1].c_str());
      return true;
    }
  }

  pinfo("BinlogReceiverThread AccessHandle failed, My server id: %ld, Master auth server id:%s",
    g_pika_port->sid(), argv[1].c_str());

  return false;
}

bool MasterConn::ProcessBinlogData(const pink::RedisCmdArgsType& argv, const PortBinlogItem& binlog_item) {
  if (!is_authed_) {
    pinfo("Need Auth First");
    return false;
  } else if (argv.empty()) {
    return false;
  }


  std::string key(" ");
  if (1 < argv.size()) {
    key = argv[1];
  }

    std::string wbuf_str;
  pink::SerializeRedisCommand(argv, &wbuf_str);
    int ret = g_pika_port->SendRedisCommand(wbuf_str, key);
  if (ret != 0) {
    pwarn("send redis command:%s, ret:%d", binlog_item.ToString().c_str(), ret);
  }

  return true;
}

int MasterConn::DealMessage(pink::RedisParser* parser, const pink::RedisCmdArgsType& argv) {
    MasterConn* conn = reinterpret_cast<MasterConn*>(parser->data);
    if (conn->binlog_header_.header_type_ == kTypeAuth) {
        return conn->ProcessAuth(argv) == true ? 0 : -1;
    } else if (conn->binlog_header_.header_type_ == kTypeBinlog) {
        return conn->ProcessBinlogData(argv, conn->binlog_item_) == true ? 0 : -1;
    }else{
        pinfo("wrong type");
    }
    return -1;
}
