// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
namespace log {

/*
log文件是按照block划分的，每个block大小是32k。用户把一条条record放入block中，record可能不能完全放入block中，故
record有4钟类型：完整的记录（即记录完整的放入了block中），记录的开头部分（即block不够放，仅放入了开头的部分)，记录的中间部分
，记录的最后部分
*/
enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,//这是一条完整的记录
  // For fragments
  kFirstType = 2,//这是一条记录的第一部分
  kMiddleType = 3,//这是一条记录的中间部分
  kLastType = 4//这是一条记录的最后部分
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768; //block大小：32k
  
// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
//一个record的header是怎么安排的。length（2字节）表示数据部分的长度
//一条record包含头部（7字节，4字节crc校验码，2字节长度（故一条record最多放64k数据（其实到不了64k，因为block只有32k），这里2字节
//仅仅表示当前record的数据长度是多少，1字节表示类型（完整的记录、记录的开头等),然后剩下的就是数据。即header+数据
static const int kHeaderSize = 4 + 2 + 1; //记录头长度：7字节
}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
