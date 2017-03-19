// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);//针对类型计算的crc2c，避免重复计算
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {//初始block偏移量为空
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {//dest_length模32k得到偏移量
  InitTypeCrc(type_crc_);
}

Writer::~Writer() {
}

Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();//添加记录数据
  size_t left = slice.size();//记录数据长度
  
  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  //对于Slice剩余长度为0的话，依然需要输出一条记录
  
  Status s;
  bool begin = true;//当前为头部
  do {
    const int leftover = kBlockSize - block_offset_;//当前block剩余多少内容
    
    /*
    *将一个记录放入block中，其中记录包括记录头和数据部分：
    *1、先看记录头能不能放下，如果不能，那么开辟一个新block
    */
    assert(leftover >= 0);
    if (leftover < kHeaderSize) { //如果剩余容量小于记录头长度
      // Switch to a new block
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));//剩余的部分填充0x00
      }
      block_offset_ = 0; //开始写一个新块，偏移量就为0了
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    //不变量：block中不会留下小于7字节的空间
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize; //当前block可用的容量
    const size_t fragment_length = (left < avail) ? left : avail;//看看剩余容量够不够放记录数据大小的
    
    RecordType type;
    const bool end = (left == fragment_length); //如果end=true：剩余容量够放记录数据大小；否则不够放记录数据大小
    if (begin && end) {
      //此时记录头还没放进去，剩余容量也够放记录数据大小的，那么就够放完整的记录即full
      type = kFullType;
    } else if (begin) {
      //此时记录头还没放进去，但剩余容量不够放记录数据大小的，那么就把记录头放进去，然后放部分数据，就是记录的第一个部分数据
      type = kFirstType;
    } else if (end) {
      //此时记录头已放进去了，剩余容量够放记录数据大小的，那么就是记录的最后部分
      type = kLastType;
    } else {
      //此时记录头已放进去，剩余容量也不够放记录数据大小的，就是数据的中间部分（不包括记录头）
      type = kMiddleType;
    }

    //将这部分数据输出，其中包括记录ptr数据，大小数据和type
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;//已放了fragment_length大小数据，向前移动这么多
    left -= fragment_length;//还有多少数据待放置
    begin = false;//记录头已经放过了
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  //当前容量肯定够放记录头和n个字节数据大小，因为由上面的AddRecord函数来保证
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];//7个字节的记录头。前4字节是crc校验，第5、6字节是数据长度，第7字节是类型
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  //对类型、数据的起始地址、数据的大小计算crc校验码
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  //先把记录头append进去，大小7字节
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    //再把数据append进去
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      //将数据缓存刷新到内核
      s = dest_->Flush();
    }
  }
  //移动block的偏移量，包括记录头和数据大小
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
