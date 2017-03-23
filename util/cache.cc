// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include "leveldb/cache.h"
#include "port/port.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {
}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
 
 
  
struct LRUHandle {
  void* value; //键值对的值
  void (*deleter)(const Slice&, void* value); //这个结构体的清除函数，由外界传进去注册
  LRUHandle* next_hash; //用于hash表冲突时使用，在哈希表中，指向哈希数组的链表中的下一个节点指针
  LRUHandle* next; //当前节点的下一个节点
  LRUHandle* prev; //当前节点的上一个节点
  size_t charge;      // TODO(opt): Only allow uint32_t? 这个节点占用的内存大小
  size_t key_length; //这个键的长度
  bool in_cache;      // Whether entry is in the cache.
  uint32_t refs;      // References, including cache reference, if present.这个节点引用次数，当引用次数为0时，即可删除
  uint32_t hash;      // Hash of key(); used for fast sharding and comparisons。这个键的哈希值
  char key_data[1];   // Beginning of key.存储键的字符串，也是C++柔性数组的概念（申请内存时可少申请一个字节）
  
  Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    //为了加速查询，有时候将key存放在valu中（当前节点的next指向自己时）
    if (next == this) {
      return *(reinterpret_cast<Slice*>(value));
    } else {
      return Slice(key_data, key_length);
    }
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
  //先是对成员初始化，然后调用Resize()，因为一开始没有哈希表，所以先给哈希表分配存储空间。
  //第一次分配时，哈希数组长度为4，当后面元素数量大于哈希表长度时，再次分配哈希表大小为现在数组长度的2倍。 
  HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  //如果ptr不为空，那么h放在ptr的后面；如果为空，那么h放在*ptr的位置
  //插入操作,先查找要插入的键值是否在哈希表中，如果在，那么用新的节点替换就的节点，并且函数返回旧的节点。
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash); //查找要插入节点的位置
    LRUHandle* old = *ptr;
    //将新节点插入哈希表中
    h->next_hash = (old == NULL ? NULL : old->next_hash);
    *ptr = h;
    if (old == NULL) {
      //此时不存在相等的节点，元素个数++
      ++elems_;
      //如果存储元素的数量大于slot个数了，那么肯定有冲突了，
      //为了保证哈希链的查找速度，尽量使平均哈希链长度为<=1。所以函数有if判断。
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);//查找要删除的节点位置
    LRUHandle* result = *ptr;//把要删除的节点地址赋值给result
    if (result != NULL) {
      *ptr = result->next_hash;//删除节点的位置赋值给删除节点的下一个节点
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_; //哈希数组的长度，即slot个数
  uint32_t elems_; //哈希数组存储元素的数量
  LRUHandle** list_; //哈希数组指针，因为数组里的元素是指针，所以类型是指针的指针
  
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  //在哈希表中，根据key和hash来找到LRUHandle*。找到一个节点它的key等参数key，或者hash等于参数hash然后返回
  //如果没找到的话，那么返回的是*ptr=NULL,这个ptr指向的是哈希数组的slot链表中的最后一个节点的下个NULL节点，可作为insert节点使用
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != NULL &&
           ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    //初始时数组有4个元素，然后进行resize翻倍直到大于elems_
    while (new_length < elems_) {
      new_length *= 2;
    }
    
    //改变数组大小后，需要对原来的元素做rehash
    LRUHandle** new_list = new LRUHandle*[new_length]; //给哈希数组分配空间
    memset(new_list, 0, sizeof(new_list[0]) * new_length);//初始全为0
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i]; //h为数组slot(i)的元素
      while (h != NULL) {
        LRUHandle* next = h->next_hash; //当前节点的下一个节点
        uint32_t hash = h->hash; //当前节点的hash值
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];//当前节点在新哈希数组的索引位置
        h->next_hash = *ptr;//头插入，将当前节点插入哈希数组的头部位置
        *ptr = h; //调整哈希数组的头部位置
        h = next;//处理当前slot的下一个元素
        count++; //处理完一个元素后递增
      }
    }
    assert(elems_ == count); //rehash处理的元素个数应该等于以前哈希表的元素个数
    delete[] list_; //删除老的哈希数组
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
//按照LRU来实现的
//一个节点要么在in-use双向链表中；要么在lru双向链表中。在in-use链表中的节点当前正在被client所使用（ref至少为2，一个为
//in-use拿着一个为client拿着）
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash,
                        void* value, size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle*list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e);

  // Initialized before use.
  size_t capacity_; //双向链表的存储容量，由各个节点的charge累加
  
  // mutex_ protects the following state.
  mutable port::Mutex mutex_;//对于多线程有效
  size_t usage_; //当前使用空间
  
  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_; //LRU链.双向循环链表的傀儡节点，不存储数据，方便定位这个链表的入口
  //ru之前(prev)的节点都是最新的节点，lru之后的节点(next)都是最“旧”的节点，所以插入新节点时，
  //就往lru.prev插入.当空间不够时，删除lru.next后的节点


  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_; //in-user链表的头节点
  
  HandleTable table_; //上面的哈希表，一个双向链表还附带一个哈希表
};

LRUCache::LRUCache()
    : usage_(0) {
  // Make empty circular linked lists.
  //空的循环链表，头节点指向自己
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_; ) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  //从lru链表中移除，放入in_use链表中
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  //如果当前引用计数为0，那么调用注册的释放函数来删除节点
  if (e->refs == 0) { // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {  // No longer in use; move to lru_ list.
    //解除e节点在双向链表中的位置，放入lru链表中
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

//把这个节点从双向链表中解除关系，不负责释放e节点
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

//将e节点插入到ist节点前面
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

//如果从哈希表中找到该节点，那么增加他的引用计数（会从lru中移除，放入in_use中），返回该节点
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != NULL) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}
  
//解引用该节点（引用计数会--）
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  //分配一个节点先
  LRUHandle* e = reinterpret_cast<LRUHandle*>(
      malloc(sizeof(LRUHandle)-1 + key.size())); //因为char key_data[1]已经拥有了一个char，所以这里可以少分配一个字节
  e->value = value;
  e->deleter = deleter; //删除函数
  e->charge = charge; //占用大小
  e->key_length = key.size(); //key
  e->hash = hash; // hash
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    //放入in_use链表中
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } // else don't cache.  (Tests use capacity_==0 to turn off caching.)

  //淘汰出一个
  //从lru节点中淘汰一个
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != NULL, finish removing *e from the cache; it has already been removed
// from the hash table.  Return whether e != NULL.  Requires mutex_ held.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != NULL) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != NULL;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

//shared表示会将请求进行load-balance
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards]; //有16个slot
  port::Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  //取hash结果的高4位作为slot的index
  static uint32_t Shard(uint32_t hash) {
    return hash >> (32 - kNumShardBits);
  }

 public:
  //对每个slot的capacity进行均分
  explicit ShardedLRUCache(size_t capacity)
      : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  virtual ~ShardedLRUCache() { }
  virtual Handle* Insert(const Slice& key, void* value, size_t charge,
                         void (*deleter)(const Slice& key, void* value)) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  virtual Handle* Lookup(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  virtual void Release(Handle* handle) {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  virtual void Erase(const Slice& key) {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  virtual void* Value(Handle* handle) {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  virtual uint64_t NewId() {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  virtual void Prune() {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  virtual size_t TotalCharge() const {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) {
  return new ShardedLRUCache(capacity);
}

}  // namespace leveldb
