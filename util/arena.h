// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <vector>
#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "port/port.h"

namespace leveldb {

// Allocate(bytes): if remaining space is enough, return alloc_ptr and update remaining space; if not-->AllocateFallback(bytes)： 
// AllocateFallback(bytes)： if large than 1k: AllocateNewBlock(bytes); else AllocateNewBlock(4k) and set remaining space
// AllocateNewBlock(bytes): alloc bytes and push it into blocks and update memory_usage_
 
//一个简易的内存分配器。会将new[]出去的内存地址放在vectro<char*>中，析构时释放这些new[]出来的内存。
//new []出一块内存后，可能用户用不完，把剩下的记录在2个变量中char* ptr, int remaining_size中，待下次分配需求来时看看能不能满足。
class Arena {
 public:
  Arena();
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  //先看看当前可用内存是否可满足，如果满足直接切割分配出去；否则调用AllocateFallback(bytes)
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return reinterpret_cast<uintptr_t>(memory_usage_.NoBarrier_Load());
  }

 private:
  char* AllocateFallback(size_t bytes);//如果bytes大于1k，分配bytes大小的内存并返回；如果小于1k，那么分配4k的内存，并切割出去，剩下的记录下来留下次用
  char* AllocateNewBlock(size_t block_bytes); //使用new[]分配参数大小的内存，并push到blocks_中

  // Allocation state
  char* alloc_ptr_; //当前可用内存地址
  size_t alloc_bytes_remaining_; //当前可用内存大小

  // Array of new[] allocated memory blocks
  std::vector<char*> blocks_; //已经分配的内存地址，通过new[]来分配的。每次都是new char[xxx]大小，所以类型是char*

  // Total memory usage of the arena.
  port::AtomicPointer memory_usage_; //像size_t memory_usage_
  // No copying allowed
  Arena(const Arena&);
  void operator=(const Arena&);
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
 //如果需要的内存大小当前内存块可满足，直接切割当前内存块并分配出去
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes; //update当前内存块的地址和大小
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
