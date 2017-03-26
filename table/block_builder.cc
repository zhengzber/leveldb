// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  //初始将0放入，第一条记录是一个restart point
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear(); //块内容清空
  restarts_.clear(); //restart points清空
  restarts_.push_back(0);       // First restart point is at offset 0，将0偏移添加到restart points
  counter_ = 0; //两个restart point间的记录数为0
  finished_ = false;//还未结束
  last_key_.clear(); //last key清空
}

//该函数主要用于判断这个块的容量是否达到上限，达到上限后，要把块刷新到磁盘，然后接着写下一块
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer ，数据容量大小
          restarts_.size() * sizeof(uint32_t) +   // Restart array，restart数组大小
          sizeof(uint32_t));                      // Restart array length, restart数组大小变量所占的字节
}

//主要是向table_builder提供返回这个块内容的接口，然后由table_builder调用函数写回磁盘。
Slice BlockBuilder::Finish() {
  // Append restart array
  //把restart数组添加到buffer后面，
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  //添加restart数组大小到restart数组后面
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true; //这次数据块写结束
  return Slice(buffer_); //向上层调用返回这个数据块的内容
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_); //上一条记录
  assert(!finished_); //这个块当前仍未结束，如果结束了外部调用add来添加记录，会报错
  assert(counter_ <= options_->block_restart_interval); //两个restart节点间的记录数小于等于预先设定的值
  assert(buffer_.empty() // No values yet? 当前块一条记录没有
         || options_->comparator->Compare(key, last_key_piece) > 0); //当前添加的记录要比上条记录要大，由skiplist有序
  size_t shared = 0;
    
  //当前记录和上条记录共享prefix key的长度  
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    //如果上述条件为false，则增加一个restart point,为buffer当前的起始地址。并且设置counter=0
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  //当前记录和上条记录的非共享部分长度
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  //key共享长度，key非共享长度,value长度
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  //key非共享内容，value内容
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  //更新上条记录为当前记录，先让last_key仅仅保存shared key的部分，然后将非shared key追加到last_key的尾部，这样效率最高
  //这样操作可能会节省一次内存拷贝，即shared key部分可能不用再拷贝了（如果非shared key太高，可能要重新分配内存那么shard key也是需要拷贝的）。
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
