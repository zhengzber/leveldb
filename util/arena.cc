// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#include <assert.h>

namespace leveldb {

static const int kBlockSize = 4096;

Arena::Arena() : memory_usage_(0) {
  alloc_ptr_ = NULL;  // First allocation will allocate a block
  alloc_bytes_remaining_ = 0;
}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

//根据需要的bytes大小看直接分配这么大内存，还是分配4k内存然后切割出去需要的
char* Arena::AllocateFallback(size_t bytes) {
  //如果需要的bytes超过1k，那么分配需要的内存出来并返回
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  //否则分配一块4k的内存大小，切割出去需要的，剩下的记录在当前可用的内存地址上
  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

//分配对齐的内存地址
char* Arena::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8; //按照8字节来对齐
  assert((align & (align-1)) == 0);   // Pointer size should be a power of 2
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align-1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod); //将alloc_ptr按照8字节对齐需要前进的偏移量
  size_t needed = bytes + slop; //当前需要的内存大小+alloc_ptr的偏移量（因为如果需要从alloc_ptr来分配那么alloc_ptr就需要前进slop来对齐）
  char* result;
  //如果当前alloc_ptr可以满足的话
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop; //result是alloc_ptr前进slop后的地址，result已按8字节对齐了
    alloc_ptr_ += needed; //update alloc_ptr和size
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    //如果不满足，那么调用Fallback来分配，会调用到new[]来分配内存，分配出来的地址是会对齐的
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align-1)) == 0);
  return result;
}

//分配一块block_bytes的内存，并将内存地址push到blocks_。返回分配的结果
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}

}  // namespace leveldb
