// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_LEVELDB_INCLUDE_SLICE_H_

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>

namespace leveldb {

// represent key in leveldb
//是leveldb中的基本的数据结构：字符串。包括长度，故可表示二进制数据。
class Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) { }//默认构造函数，空的Slice。用法：Slice slice;

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) { } //代表d[0...n-1]

  // Create a slice that refers to the contents of "s"
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) { } //用string来初始化

  // Create a slice that refers to s[0,strlen(s)-1]
  Slice(const char* s) : data_(s), size_(strlen(s)) { } //代表s[0...strlen(s)]

  // Return a pointer to the beginning of the referenced data
  const char* data() const { return data_; } //返回起始地址

  // Return the length (in bytes) of the referenced data
  size_t size() const { return size_; } //返回大小

  // Return true iff the length of the referenced data is zero
  bool empty() const { return size_ == 0; } //字符串是否为空

  // Return the ith byte in the referenced data.
  // REQUIRES: n < size()
  char operator[](size_t n) const { //返回第n个字符，有断言n要小于字符串长度
    assert(n < size());
    return data_[n];
  }

  // Change this slice to refer to an empty array
  void clear() { data_ = ""; size_ = 0; } //清空字符串，其实daga等于""或者NULL都可以

  // Drop the first "n" bytes from this slice.
  //抛弃前n个字节，断言n要小于字符串长度。将data前移n，长度减去n
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Return a string that contains the copy of the referenced data.
 //返回一个string类型。
  std::string ToString() const { return std::string(data_, size_); }

  // Three-way comparison.  Returns value:
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  //Slice x是否是前缀，判断长度大于x的长度，用memcmp比较前n个字节的是否相等即可。
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) &&
            (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  const char* data_; //字符串的地址（const成员必须在构造函数的初始化列表中初始化）
  size_t size_; //长度

  // Intentionally copyable
};

//非类成员，比较2个Slice是否相等。即长度相等&所有字节相等
inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

//比较两个Slice的大小，优先比较字节内容，如果min(size，xx)的字节内容相同，再比较长度
inline int Slice::compare(const Slice& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_) r = -1;
    else if (size_ > b.size_) r = +1;
  }
  return r;
}

}  // namespace leveldb


#endif  // STORAGE_LEVELDB_INCLUDE_SLICE_H_
