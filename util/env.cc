// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"

//主要是把一些对象的析构函数放在这里
namespace leveldb {

Env::~Env() {
}

Status Env::NewAppendableFile(const std::string& fname, WritableFile** result) {
  return Status::NotSupported("NewAppendableFile", fname);
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

Logger::~Logger() {
}

FileLock::~FileLock() {
}

void Log(Logger* info_log, const char* format, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync) {
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);//创建一个可写的文件，对应fname
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data); //将data写入到文件中
  if (s.ok() && should_sync) {
    s = file->Sync(); //刷新到磁盘
  }
  if (s.ok()) {
    s = file->Close(); //关闭文件句柄
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->DeleteFile(fname);
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  data->clear();
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];  //一个8k的buffer
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);//每次最多读8k到space中
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size()); //将buffer数据append到string*data中
    //如果fragment是空的，那么file已读完
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {
}

}  // namespace leveldb
