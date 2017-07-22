// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
//meta block存放了bloom filter信息，这样可以减少磁盘读取
//构建一个filter block,包括filter1...filtern, filter1偏移....filtern偏移，filter偏移首地址
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();
 
  //其实key_和start_都是为了AddKey而存在的，添加key时，会把key追加到key_后面并把相应偏移放入start_中，生成filter时会从key_和start_数组
  //复原出来各个key并放入tmp_keys_中。其实不用key_和start_也可以，Addkey时直接将key添加到tmp_keys_中也可以，这样就不用复原了，可能潜在的
  //的问题是vector需要不断重新分配内存！
  const FilterPolicy* policy_;    //过滤策略，比较有名的是布隆过滤器
  std::string keys_;              // Flattened key contents：当前filter包含的键值
  std::vector<size_t> start_;     // Starting index in keys_ of each key：当前Filter包含键值的首地址偏移量，相对于keys_首地址来说
  std::string result_;            // Filter data computed so far: 目前为止filter block构建出的内容
  std::vector<Slice> tmp_keys_;   // policy_->CreateFilter() argument： 临时slice向量，用于向result_添加本次keys_数据
  std::vector<uint32_t> filter_offsets_; //每个filter的偏移量
 
  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

class FilterBlockReader {
 public:
 // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_; //过滤策略，leveldb选择的是bloom filter，用户可自定义
  const char* data_;    // Pointer to filter data (at block-start) filter-block的起始地址
  const char* offset_;  // Pointer to beginning of offset array (at block-end) 当前filter-block中filter偏移数组的起始位置
  size_t num_;          // Number of entries in offset array //当前filter-block中filter数目
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
