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

//log的类型
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
//一个block的header是怎么安排的。length（2字节）表示数据部分的长度
static const int kHeaderSize = 4 + 2 + 1; //记录头长度：7字节
}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
