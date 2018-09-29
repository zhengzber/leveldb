// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <stdint.h>
#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "port/port.h"
#include "util/logging.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

 /*
 如果start<limit，将start修改为最短的news_start并且满足start<=new_start<limit，
 方法是news_start保留start和limit的共同前缀，然后再尝试append一个字符，
 这个字符是start和limit第一个非前缀字符+1，这样来保证start<=new_start<limit。
例如：*start=helloworld，limit=hellozookeeper，因为start<limit，共同前缀是hello，
start中和limit第一个非前缀字符是w，w+1=x那么新字符new_start=hellox，这样start<new_start<limit。
这个函数用在index blocks处， we donot emit the index entry for a block until we have seen the 
first key for the next data block, this allows us to use shorter keys in the index block. 
For example, consider a block boundary between the keys "the quick brown fox" and "the who", 
we can use "the r" as the key for the index block entry since it is >= all entries int 
the first block and  < all entries in subsequent blocks。实现细节：先找到2者直接的common prefix，
如果一个字符串是另外一个前缀，那么啥也不做；否则start的第一个不同的字节是diff_byte，
如果diff_byte<0xff（防止diff_byte+1溢出了）并且diff_byte+1小于limit的第一个不同的byte，
那么将start的该处字节+1，然后截断start到该字节处并返回
 
 */
  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }
   //先找到common prefix

    if (diff_index >= min_length) {
     //这种情况下，一个字符串会是另外一个的prefix
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]); //获得start非共同前缀的第一个字符
      if (diff_byte < static_cast<uint8_t>(0xff) && //因为后面要加1，这里判断是防止溢出
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) { //并且要小于limit的那个字符
        (*start)[diff_index]++;//非共同前缀字符+1
        start->resize(diff_index + 1); //截断字符
        assert(Compare(*start, limit) < 0); //确保start<=new_start<limit
      }
    }
  }

 //找到一个比key大的最短的字符串，并且就地改为key
 //把key修改成一个比key大的短字符串，实现方法是顺着key的第一个字符找，直到找到一个不是0xff的字符然后把该字符加1，并截断
  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) { //找到第一个不是0xff的字符
        (*key)[i] = byte + 1; //将该字符加1
        key->resize(i+1);//然后做截断
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

static port::OnceType once = LEVELDB_ONCE_INIT;
static const Comparator* bytewise;

static void InitModule() {
  bytewise = new BytewiseComparatorImpl;
}

const Comparator* BytewiseComparator() {
  port::InitOnce(&once, InitModule);
  return bytewise;
}

}  // namespace leveldb
