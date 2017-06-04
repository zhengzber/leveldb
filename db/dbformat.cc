// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "db/dbformat.h"
#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

//8字节共64位二进制，其实高56位放seq,低8位放类型，返回一个uint64_t
//高58位放入sequence number, 低8位放入valuetype
//即将sequence number和value_type组成8字节返回（高位7字节放sequence number，低1字节放value_type）
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

//将ParsedInternalKey序列化成InternalKey，append到result中。即先把user_key放入result，然后把sequence number和value_type放入8字节中，
//然后8字节编码成fixed char buf[8]中，然后result中
void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

std::string ParsedInternalKey::DebugString() const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' @ %llu : %d",
           (unsigned long long) sequence,
           int(type));
  std::string result = "'";
  result += EscapeString(user_key.ToString());
  result += buf;
  return result;
}

std::string InternalKey::DebugString() const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString();
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

//akey和bkey都是InternalKey
//先比较user_key，如果相等；那么比较sequence number（这里直接比较uint64_t，如果sequence_number也相等，
//那么隐含地表示将会比较value_type即低8位）
//先按user_key的升序排序；如果相等，然后按照sequence number的降序排序；如果相等，那么按照value_type的降序排序
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp, PackSequenceAndType(kMaxSequenceNumber,kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const {
  return user_policy_->Name();
}

void InternalFilterPolicy::CreateFilter(const Slice* keys, int n,
                                        std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);
  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  //kLength占5个字节，sequence number和value_type
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  //如果需要的内存小于等于200字节，那么直接使用space_，否则从堆上分配
  //这个设计挺好的，如果内存较小，没必要从堆上分配，直接使用内部的缓存即可，而且200字节也不大，避免了较小的key也从堆上分配的负担
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  //start_指向整个内存的开始处
  start_ = dst;
  //将user_key的大小和后面tag的8字节大小累加，放入vaint32中，放在dst的头部5字节中
  dst = EncodeVarint32(dst, usize + 8);
  //kstart_指向user_key开始的char*
  kstart_ = dst;
  //将user_key的数据拷贝到这里，此时dst=kstart_=user_key应该开始放置的位置
  memcpy(dst, user_key.data(), usize);
  //跳过user_key的大小，指向tag的起始处
  dst += usize;
  //将sequence_number和普通类型value_type序列化为二进制然后作为tag放到整个内存的最后8字节处
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  //end_指向整个内存的末尾的下一个字节处 [start_, end_)表示这块内存
  end_ = dst;
}

}  // namespace leveldb
