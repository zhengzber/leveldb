// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator: public Iterator {
 public:
  TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options);

  virtual ~TwoLevelIterator();

  virtual void Seek(const Slice& target);
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Next();
  virtual void Prev();

  virtual bool Valid() const {
    return data_iter_.Valid();
  }
  virtual Slice key() const {
    assert(Valid());
    return data_iter_.key();
  }
  virtual Slice value() const {
    assert(Valid());
    return data_iter_.value();
  }
  virtual Status status() const {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != NULL && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_; //就是传进来的BlockReader函数
  void* arg_;//Table指针
  const ReadOptions options_;//读取配置选项
  Status status_;
  IteratorWrapper index_iter_;//index block迭代器，就是上述第一个参数
  IteratorWrapper data_iter_; // May be NULL  可能为空，读取某个data block的迭代器
  // If data_iter_ is non-NULL, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;//用于判断两次data_iter是否相等。
};

TwoLevelIterator::TwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(NULL) {
}

TwoLevelIterator::~TwoLevelIterator() {
}

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);//找到target所以块的handle
  InitDataBlock();//初始化data_iter_，即指向target所在的块首位置
  if (data_iter_.iter() != NULL) data_iter_.Seek(target);//如果没报错即找到了，通过data_iter_.value即可获取值。
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();//index_block迭代器指向第一条记录，也就是第一个块的blockhandle
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToFirst();
  }
}

void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == NULL || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(NULL);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != NULL) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != NULL) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(NULL);
  } else {
    Slice handle = index_iter_.value();//指向第一个块的handle
    if (data_iter_.iter() != NULL && handle.compare(data_block_handle_) == 0) {
     // data_iter_不为空，而且还是相同块的迭代器，所以不变
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter = (*block_function_)(arg_, options_, handle);//调用BlockReader，获取这个块的迭代器。
      data_block_handle_.assign(handle.data(), handle.size());//为data_block_handle_赋值，用于判断两次data_iter_是否相等
      SetDataIterator(iter);//设置data_iter_
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    BlockFunction block_function,
    void* arg,
    const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
