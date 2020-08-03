// Copyright (c) 2018-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef BINLOG_TRANSVERTER_H_
#define BINLOG_TRANSVERTER_H_

#include <iostream>
#include <vector>

#include "slash/include/slash_coding.h"

/*
 * **************Header**************
 *  | <Transfer Type> | <Body Lenth> |
 *       2 Bytes         4 Bytes
 */
#define HEADER_LEN 6


enum TransferOperate{
    kTypeAuth = 1,
    kTypeBinlog = 2
};

#define BINLOG_ENCODE_LEN 34

struct BinlogHeader {
    uint16_t header_type_;
    uint32_t item_length_;
    BinlogHeader() {
        header_type_ = 0;
        item_length_ = 0;
    }
};

const int BINLOG_ITEM_HEADER_SIZE = 34;
const int PADDING_BINLOG_PROTOCOL_SIZE = 22;
const int SPACE_STROE_PARAMETER_LENGTH = 5;




/*
 * *****************Type First PortBinlog Item Format*****************
 * | <Type> | <Create Time> | <Server Id> | <PortBinlog Logic Id> | <File Num> | <Offset> | <Content Length> |      <Content>     |
 *  2 Bytes      4 Bytes        4 Bytes          8 Bytes         4 Bytes     8 Bytes         4 Bytes      content length Bytes
 *
 */

enum PortBinlogType {
  PortTypeFirst = 1,
};

class PortBinlogItem {
  public:
    PortBinlogItem() :
        exec_time_(0),
        server_id_(0),
        logic_id_(0),
        filenum_(0),
        offset_(0),
        content_("") {}

    friend class PortBinlogTransverter;

    uint32_t exec_time()   const;
    uint32_t server_id()   const;
    uint64_t logic_id()    const;
    uint32_t filenum()     const;
    uint64_t offset()      const;
    const std::string& content() const;
    std::string ToString() const;

    void set_exec_time(uint32_t exec_time);
    void set_server_id(uint32_t server_id);
    void set_logic_id(uint64_t logic_id);
    void set_filenum(uint32_t filenum);
    void set_offset(uint64_t offset);

  private:
    uint32_t exec_time_;
    uint32_t server_id_;
    uint64_t logic_id_;
    uint32_t filenum_;
    uint64_t offset_;
    std::string content_;
    std::vector<std::string> extends_;
};

class PortBinlogTransverter{
  public:
    PortBinlogTransverter() {};
    static std::string PortBinlogEncode(PortBinlogType type,
                                    uint32_t exec_time,
                                    uint32_t server_id,
                                    uint64_t logic_id,
                                    uint32_t filenum,
                                    uint64_t offset,
                                    const std::string& content,
                                    const std::vector<std::string>& extends);

    static bool PortBinlogDecode(PortBinlogType type,
                             const std::string& binlog,
                             PortBinlogItem* binlog_item);



    static std::string ConstructPaddingBinlog(PortBinlogType type, uint32_t size);

    static bool BinlogHeaderDecode(PortBinlogType type,
                                   const std::string& binlog,
                                   BinlogHeader* binlog_header);
    static bool BinlogItemWithoutContentDecode(PortBinlogType type,
                                               const std::string& binlog,
                                               PortBinlogItem* binlog_item);




};

#endif

