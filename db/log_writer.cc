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

//根据用户的slice产生一条record
Status Writer::AddRecord(const Slice& slice) {
  const char* ptr = slice.data();//记录数据起始地址
  size_t left = slice.size();//记录数据长度
  
  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  //对于Slice剩余长度为0的话，依然需要输出一条记录
  
  Status s;
  bool begin = true;//当前为头部
  do {
    const int leftover = kBlockSize - block_offset_;//当前block剩余多少长度的容量
    
    /*
    *将一个记录放入block中，其中记录包括记录头和数据部分：
    *1、先看记录头能不能放下，如果不能，那么开辟一个新block
    */
    assert(leftover >= 0);
    if (leftover < kHeaderSize) { //如果剩余容量小于记录头长度，即小于7字节
      // Switch to a new block
      //填充剩余的部分为0
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));//剩余的部分填充0
      }
      block_offset_ = 0; //开始写一个新块，偏移量就为0了
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    //不变量：block中不会留下小于7字节的空间
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize; //当前block可用的容量长度
    const size_t fragment_length = (left < avail) ? left : avail;//剩余容量可放入的当前数据的大小
    
    //注意如果left是0的话，那么也会写入一条记录的，因为函数入口没有对left=0的请求直接返回。
    
    RecordType type;
    const bool end = (left == fragment_length); //如果end=true：剩余容量够放记录数据大小；否则不够放记录数据大小
    if (begin && end) {
      //第一次进入这个循环，并且剩余容量够放数据大小，那么把完整的记录放进去，即类型是full
      type = kFullType;
    } else if (begin) {
      //第一次进入这个循环，并且剩余容量不够放数据大小，那么把数据的前面部分放进去，即类型是first
      type = kFirstType;
    } else if (end) {
      //不是第一次进入这个循环，即用户数据已经放了一部分进去了，如果剩余容量够放数据，那么就是最后的记录了，即last
      type = kLastType;
    } else {
      //不是第一次进入这个循环，即用户数据已经放了一部分进去了，并且剩余容量不够放其他数据的，那么就把剩余容量填充一部分数据，作为中间的记录，即middle
      type = kMiddleType;
    }

    //将当前block剩余的容量填充好用户数据，用户数据是ptr，放进去的大小是fragment_length，类型是type
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;//已放了fragment_length大小数据，用户数据向前移动
    left -= fragment_length;//用户数据还剩多少
    begin = false;//用户数据已经放过一部分了，不是是记录头了
  } while (s.ok() && left > 0);
  return s;
}

//先把header append到文件，然后把数据append到文件，然后刷新文件落盘，全部操作都ok才返回ok。
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  //当前容量肯定够放记录头和n个字节数据大小，因为由上面的AddRecord函数来保证
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];//7个字节的记录头。前4字节是crc校验，第5、6字节是数据长度，第7字节是类型
  buf[4] = static_cast<char>(n & 0xff); //注意buf[4]放长度的低位8字节，buf[5]放长度的高位8字节，是小端模式
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
  //注意如果放入头部7字节失败后，Block_offset也会移动7字节+n的，相当于这块7字节+n的内容被占用了，这样不至于破坏其它block的结构。
  //移动block的偏移量，包括记录头和数据大小
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
