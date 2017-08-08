// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//在12个字节之后，就是一条条的记录了。
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

//留12字节大小作为头部
void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

//参数是Handler,实现类是MemTableInsert*，包含put(key, value)和delete(key)接口，即将key,value放入memtable或从memtable中删除（其实
//实现是添加一条删除记录，而不是真正的删除，只在compaction时才会去删除该记录）
//这个方法解析出一条记录，然后判断记录的类型，根据不同的类型，
//调用Hander不同的方法将记录插入memtable中。这个方法最后由WriteBatchInternal这个类的InsertInto调用；
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  if (input.size() < kHeader) { //至少大于等于12字节
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  //移除12字节头部
  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    char tag = input[0];
    input.remove_prefix(1);//移除第一个字节，这个字节是类型
    switch (tag) {
      case kTypeValue:
        //如果是key,value类型，拿出key, value然后调用handler的put接口放入memtable中
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        //如果是delete类型，拿出key，调用handler的delete接口，将删除键类型加入memtable中
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  //如果发现的记录数不等于这个writebatch里count字段即记录数字段，返回失败
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

//添加key, value
void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);//先增加记录数
  rep_.push_back(static_cast<char>(kTypeValue));//添加key,value类型
  PutLengthPrefixedSlice(&rep_, key);//添加key
  PutLengthPrefixedSlice(&rep_, value);//添加value
}

//添加一条记录，为删除类型，key
void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);//新增加记录数
  rep_.push_back(static_cast<char>(kTypeDeletion));//添加删除类型
  PutLengthPrefixedSlice(&rep_, key);//添加key
}

namespace {
//这个类就是将key,value放入memtable中
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

//遍历writebatch的所有记录，然后根据记录类型调用MemtableInsert的不同接口，将记录添加到memtable中
Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

//设置writebatch的内容
void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}
//将src中的所有记录添加进dst中
void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));//设置dst的记录数位dst和src的记录数之和
  assert(src->rep_.size() >= kHeader);
  //将src头部之后的记录数据全部append到dst中
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
