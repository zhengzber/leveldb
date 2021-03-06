// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

//传入的参数一般是varint32+value，前面最多5个字节是数据大小，后面是数据。该函数解析出大小后，将后面数据放入Slice返回
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp), //InternalKeyComparator来初始化comparator_
      refs_(0), //引用计数初始化为0
      table_(comparator_, &arena_) //初始化skiplist
      {
}

MemTable::~MemTable() {
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }//返回内存池的大概内存，skiplist的节点内存需要从arena_中分配

//aptr和bptr都是LookupKey，先移除前面的internal-key-size，然后比较2个internal-key
int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr)
    const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
 ////InternalKeyComparator.注意这里的aptr,bptr包括了后面附加的8字节信息.
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
//将一个internal-key target encode成LookupKey放入scratch中
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

//基本上memtable的迭代器是一个代理类，调用的都是memtable内部skiplist的迭代器的方法.
class MemTableIterator: public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) { }

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice& k) { iter_.Seek(EncodeKey(&tmp_, k)); } //k是internal-key，先encode成LookupKey，然后调用skiplist的Seek在skiplist中查找
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); } //返回的是Internal-key
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());//先获得Internal-key
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());//返回的是value
  }

  virtual Status status() const { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_; //skiplist的迭代器
  std::string tmp_;       // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator&);
  void operator=(const MemTableIterator&);
};

Iterator* MemTable::NewIterator() {
  return new MemTableIterator(&table_);
}

//将key, value, s, type打包成skiplist里的存储结构（const char*）：internal_key_size+internal_key+value_size+value
//然后插入到skiplist中（这个存储结构需要从arena分配一块内存）
void MemTable::Add(SequenceNumber s, ValueType type,
                   const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size(); //键的大小
  size_t val_size = value.size(); //值得大小
  size_t internal_key_size = key_size + 8;// internal key的大小 user_key_size+8字节（8字节用来序列化sequence_number+value_type）
  //整个存储需要的字节数，注意已加8字节将user_key变成了InternalKey
  const size_t encoded_len = //总大小
      VarintLength(internal_key_size) + internal_key_size +
      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);//从内存分配器中分配一块连续的内存
  char* p = EncodeVarint32(buf, internal_key_size);  //先将internal_key_size给放入buf中，p为放过后的起始空闲位置
  memcpy(p, key.data(), key_size); //将user_key给放过去
  p += key_size;//这里准备放入8字节的信息
  EncodeFixed64(p, (s << 8) | type);//把8字节的sequence_num+value_type给放进去
  p += 8;
  p = EncodeVarint32(p, val_size);//将val_size也放进去
  memcpy(p, value.data(), val_size);//把val给放进去
  assert((p + val_size) - buf == encoded_len);//这里应该相等，一块内存正好用完
  table_.Insert(buf); //将这个buf插入skiplist中
}

//skiplist里存的是InternalKey_size+InternalKey+val_size+val。LoopupKey是InternalKey_size+InternalKey。
//整体思路是根据LookupKey在skiplist中找到greaterorequal当前LookupKey的，然后比较前面的部分即InternalKey_size+InternalKey是否相等
//如果不相等，那么返回false；如果相等，再把InternalKey解析出来，再从InternalKey解析出最后的8字节找到sequence+value_type，如果value_type
//是普通类型，那么解析出val然后赋值返回true；如果value_type是删除类型，那么设置status然后返回true
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  Slice memkey = key.memtable_key();//获得memtable_key，即internal_key_size+internal_key
  Table::Iterator iter(&table_);//在table(skiplist)上的迭代器
  iter.Seek(memkey.data());//寻找memkey
  if (iter.Valid()) { //如果找到的node不为NULL的话，那么找到一个和memkey相等或比它大的node(因为存储在skiplist的是internal_key_size+internal_key
    //+value_size+value，所以拿internal_key_size+internal_key去match key的话，可能找到前缀匹配的，但真正key并不相同的)
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    //这里不需要再检查sequence number了，因为seek已经跳过了所有值更大的sequence number了（因为InternalKeyComparator的比较函数是先按照user_key的
    //升序来比较，然后按照sequence number的降序来比较）
    const char* entry = iter.key();//table中的key（结构为InternalKey_size+InternalKey+value_size+value）
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry+5, &key_length);//获得InternalKey的size
    //如果InternalKey的user_key（整个InternalKey大小减去8个字节）和LookupKey的user_key相等
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8),
            key.user_key()) == 0) {
      // Correct user key
      //从InternalKey的最后8字节解析出sequence number和value_type
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        //如果是普通类型
        case kTypeValue: {
          //此时key_ptr+key_length指向的位置是val_size+val。通过GetLengthPrefixedSlice来解析出v
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());//将v赋值给输出参数
          return true;
        }
        //如果是删除类型
        case kTypeDeletion:
          //设置下状态
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
