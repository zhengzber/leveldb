// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <stdio.h>
#include "port/port.h"
#include "leveldb/status.h"

namespace leveldb {

//将state的内容也拷贝一份给当前state，需要分配内存。
const char* Status::CopyState(const char* state) {
  //先看看state的长度有多少，取前4个字节拿出长度
  uint32_t size;
  memcpy(&size, state, sizeof(size));
  //分配state需要的长度，即前4字节内容+5字节
  char* result = new char[size + 5];
  //将state的所有内容(size+5长度)都拷贝到result中
  memcpy(result, state, size + 5);
  return result;//返回result
}

//设置errcode为code， err_msg为msg和msg2的err_msg的拼接，中间用2个字节": "来分隔（如果msg2的err_msg不为空的话）。返回新的Status
Status::Status(Code code, const Slice& msg, const Slice& msg2) {
  assert(code != kOk);//用断言判断code不是ok，否则直接返回一个默认构造的Status就行啦，因为默认构造的status的state_=NULL就表示ok
  const uint32_t len1 = msg.size();
  const uint32_t len2 = msg2.size();
  const uint32_t size = len1 + (len2 ? (2 + len2) : 0); //如果msg2的err_msg不为空，整体size是2个size+2字节（": ")；如果为空就是msg1的size大小
  char* result = new char[size + 5]; //为err_msg申请一块内存
  memcpy(result, &size, sizeof(size)); //将size放入state的前4个字节中
  result[4] = static_cast<char>(code); //将code放入state的第5个字节中
  memcpy(result + 5, msg.data(), len1);//将msg1的err_msg的内容放到state中
  if (len2) {
    //如果msg2的err_msg不为空，那么级那个msg2的err_msg放state中，和msg1的err_msg用2字节内容（一个:一个空格）来分隔
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    //将msg2的err_msg拷贝到state_中
    memcpy(result + 7 + len1, msg2.data(), len2);
  }
  //设置state_为result
  state_ = result;
}

//用string来表示Status。一个string前面先放代码状态码的字符串，然后后面放err_msg
std::string Status::ToString() const {
  if (state_ == NULL) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case kOk:
        type = "OK";
        break;
      case kNotFound:
        type = "NotFound: ";
        break;
      case kCorruption:
        type = "Corruption: ";
        break;
      case kNotSupported:
        type = "Not implemented: ";
        break;
      case kInvalidArgument:
        type = "Invalid argument: ";
        break;
      case kIOError:
        type = "IO error: ";
        break;
      default:
        snprintf(tmp, sizeof(tmp), "Unknown code(%d): ",
                 static_cast<int>(code()));
        type = tmp;
        break;
    }
    std::string result(type); //将状态码表示的字符串放入result中
    uint32_t length;
    memcpy(&length, state_, sizeof(length)); //获得err_msg的长度
    result.append(state_ + 5, length); //将err_msg拷贝到result中
    return result;//返回result
  }
}

}  // namespace leveldb
