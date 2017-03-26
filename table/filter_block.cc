// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
// 2kB数据
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {
}

//leveldb是这么分配filter block的.以base(2KB)计算.如果block offset在[base*i,base*(i+1)-1]之间的话，那么就在filter i上面
//可以看到两个data block offset跨越超过base的话那么会产生几个empty filter.但是默认实现的话empty filter不占用太多空间。 
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  //假设filter_index=3, 那么block_offset对应的数据将映射到filter3。假设filter_offsets.size=1，即当前只有1个filter1
  //那么调用GenerateFilter来创建出filter2, filter3
  while (filter_index > filter_offsets_.size()) {
    //创建filter条目
    GenerateFilter();
  }
}

//添加key到当前filter中，首先将keys_的大小放入start，然后将key放入keys后面。这样start[i]+keys_.data()就是第i个key的起始地址，第i
//个key的长度是start[i+1]-start[i],
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

//搞定一个filter block(meta block)
Slice FilterBlockBuilder::Finish() {
  //如果start_不为空，那么最后一个filter还没写完，此时生成一个filter将剩下的key写进去
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();//result的大小，此时result包含的数据都是key进行bloomfilter计算过的数据，即filter1...filtern
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    //将每个filter的偏移量追加到result的后面，即将filter1...filtern的偏移量追加到result后面
    PutFixed32(&result_, filter_offsets_[i]);
  }

  PutFixed32(&result_, array_offset);//将filter偏移数组的首地址追加到result后面
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result 将2KB这个数字追加到result后面
  return Slice(result_);//返回整个result，此时result就是一个完整的meta block
}

//创建一个filter
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size(); //当前filter中key的个数
  if (num_keys == 0) {
    //如果为空，那么当前filter的key数量为空，那么将创建一个空的filter，即将当前filter的偏移量放入filter_offsets中
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  //从keys_和start_中复原所有添加的key，并且都添加到tmp_keys中
  start_.push_back(keys_.size());  // Simplify length computation，为了方便计算最后一个key的长度，此时start_数组的元素个数是num_keys+1
  tmp_keys_.resize(num_keys); //有num_keys个key，所以先对tmp_keys_进行resize
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];//第i个key的起始地址
    size_t length = start_[i+1] - start_[i];//第i个key的长度
    tmp_keys_[i] = Slice(base, length);//复原出来一个key，放入tmp_keys中
  }

  // Generate filter for current set of keys and append to result_.
  filter_offsets_.push_back(result_.size());//当前filter的偏移量放入filter_offsets中
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);//将num_keys个key进行bloom filter计算，计算出一组string然后
    //append到result后面
  tmp_keys_.clear(); //临时存储key的清空掉，方便下个filter的临时存储
  keys_.clear();//临时存储所有key清空掉
  start_.clear();//临时存储所有key的偏移量清空掉
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy),
      data_(NULL),
      offset_(NULL),
      num_(0),
      base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n-1];
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index*4);
    uint32_t limit = DecodeFixed32(offset_ + index*4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}
