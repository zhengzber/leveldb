// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <vector>

#include <stdint.h>
#include "leveldb/slice.h"

/*
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
*/

namespace leveldb {

struct Options;

//构造一个块的，将多个有序的kv写到一个连续内存块中。提供的reset接口允许BlockBuilder重复使用，底层对key的prefix部分进行了压缩
//对于每K个key的话会保存一个完整key,然后对于 剩余的K-1个key采用prefix-compressed的方式压缩。共享的部分长度叫做shared_bytes,
 //非共享的部分叫做unshared_bytes. 对于保存这些完整的key的点，叫做restarts
class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  // Reset the contents as if the BlockBuilder was just constructed.
  //重置BlockBuilder的各个属性，便于下一次写
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  //往当前块添加一条记录
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  //当前块写结束，返回这个块的所有内容，在tablebuilder写入文件
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  //估计这个块的数据量，用于判断当前块是否大于option当中定义数据块大小
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options*        options_;   //Option类，由最上层open函数传进来，这里主要用于counter_ < options_->block_restart_interval)
                                    //判断两个Restart节点之间，记录数量是否小于option定义的值
  std::string           buffer_;      // Destination buffer 这个块的所有数据，数据一条一条添加到这个string中
  std::vector<uint32_t> restarts_;    // Restart points 存储每个restart[i], restart[i]记录的是相对索引，即restart[0]=0第一条非共享记录的地址是0
  //restart[i]=t表示第i个非共享记录的起始地址是当前块的地址+t
  int                   counter_;     // Number of entries emitted since restart 两个restart之间记录的条数
  bool                  finished_;    // Has Finish() been called? 是否调用finish，即是否写完这个块
  std::string           last_key_;    //每次写记录时的上一条记录，用于提供共享记录部分

  // No copying allowed
  BlockBuilder(const BlockBuilder&);
  void operator=(const BlockBuilder&);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
