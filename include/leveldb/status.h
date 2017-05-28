// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Status encapsulates the result of an operation.  It may indicate success,
// or it may indicate an error with an associated error message.
//
// Multiple threads can invoke const methods on a Status without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Status must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_STATUS_H_
#define STORAGE_LEVELDB_INCLUDE_STATUS_H_

#include <string>
#include "leveldb/slice.h"

namespace leveldb {

//ErrCode和ErrMsg的封装，都用一个字符数组来表示。整体用类来封装。
//如果state_是NULL表示状态ok，其它情况需要分配内存到state_中来表示异常情况：这样的好处是如果状态大多数是ok的话，那么不需要从堆中分配任何内存了
//只在异常状态下才会需要从堆中分配内存，可以节省一点内存申请。
class Status {
 public:
  //注意stata_是NULL的时候，表示Status是ok，即状态正常。其它情况都表示状态异常，异常code是第5个字节，异常msg是第6个字节开始的字符串。
  // Create a success status.
  Status() : state_(NULL) { } //默认构造函数创建一个空的Status，字符串为NULL，表示状态是ok
  ~Status() { delete[] state_; } //析构函数删除字符串内存，所以一般的构造函数要分配内存

  // Copy the specified status.
  Status(const Status& s);
  void operator=(const Status& s);

  // Return a success status.
  static Status OK() { return Status(); } //返回一个OK的status，就是默认构造函数即可。

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }

  // Returns true iff the status indicates success.
 //state_==NULL时表示everything is ok
  bool ok() const { return (state_ == NULL); } //

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  // Returns true iff the status indicates a NotSupportedError.
  bool IsNotSupportedError() const { return code() == kNotSupported; }

  // Returns true iff the status indicates an InvalidArgument.
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

 private:
  // OK status has a NULL state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_; //前4个字节是state的err_msg的长度，第5个字节表示错误码Code，第6个字节开始是err_msg。所以整体state_的长度是前4个字节
 //所表示的长度+5字节

  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5
  };

 //返回状态码，如果state_是NULL就返回ok，否则返回4个字节（将字节转换成Code）。
  Code code() const {
    return (state_ == NULL) ? kOk : static_cast<Code>(state_[4]);
  }

  //这也是一个构造函数，状态吗为code， err_msg为msg和msg2的err_msg的拼接（中间用：和空格来分隔），需要分配内存。
  //一般err_msg都较小，不然得话在构造函数中分配内存不太好。
  Status(Code code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);
};

//复制构造函数，如果s.state_==NULL那么当前state_也设置为NULL，都表示状态ok。
//否则分配一块内存，来放置s的state_
inline Status::Status(const Status& s) {
  state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_);
}
//赋值构造函数
inline void Status::operator=(const Status& s) {
  // The following condition catches both aliasing (when this == &s),
  // and the common case where both s and *this are ok.
  //先判断2个state_是否相等（如果都是NULL那么也会match this condition)。
  if (state_ != s.state_) {
    //否则，删除本state_的内存，然后将s.state_的内存拷贝到state_
    delete[] state_;
    state_ = (s.state_ == NULL) ? NULL : CopyState(s.state_); //这里还是需要判断s.state是否为NULL的，如果为NULL就不用分配内存拉。
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_STATUS_H_
