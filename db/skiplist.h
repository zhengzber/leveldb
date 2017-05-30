// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <assert.h>
#include <stdlib.h>
#include "port/port.h"
#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template<typename Key, class Comparator>
//跳跃表，作为memtable的内部结构，存储的key是有序的。对外暴露的接口只有两个，分别是插入节点，和判断某个key是否在skiplist中。
//包含一个迭代器，提供seek来定位到某个key，和seekToFirst和seekToLast，Next和Prev的方法。
//由于跳跃表的每层的单向链表来实现的，故Prev方法不高效需要O(logN)时间来遍历到前一个节点
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  //构造函数带参数内存分配器和key comparator
  explicit SkipList(Comparator cmp, Arena* arena);

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  //插入key到skiplist，对外暴露的接口之一
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  //这个skiplist是否包含这个key，对外暴露的接口之一
  bool Contains(const Key& key) const;

  // Iteration over the contents of a skip list
  //skiplist的迭代器，迭代器指向一个节点，迭代器会遍历skiplist然后通过迭代器会找到某个节点
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    //迭代器的构造函数是skiplist
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    //当前迭代器是否指向一个valid的节点
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    //当前迭代器指向的key
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    //迭代器到下一个节点
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    //迭代器的上一个节点
    void Prev();

    // Advance to the first entry with a key >= target
    //到第一个节点which key>=target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //直接到skiplist的第一个节点
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    //直接到skiplist的最后一个节点
    void SeekToLast();

   private:
    const SkipList* list_; //成员是const，必须在构造函数的初始化列表中进行初始化
    Node* node_; //当前迭代器指向的节点
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 }; //这种用法表示类中的常量，表示skiplist的最大高度

  // Immutable after construction
  Comparator const compare_; //key的比较器，成员之一
  Arena* const arena_;    // Arena used for allocations of nodes 内存分配器，成员之一

  Node* const head_; //skiplist的头节点，是个dummy节点，不指向有效的key

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  port::AtomicPointer max_height_;   // Height of the entire list //当前的高度，随着insert和delete可能会update

  //返回当前高度
  inline int GetMaxHeight() const {
    return static_cast<int>(
        reinterpret_cast<intptr_t>(max_height_.NoBarrier_Load()));
  }

  // Read/written only by Insert().
  Random rnd_; //随机数生成器，对于一个新的insert key，需要随机生成它的高度

  Node* NewNode(const Key& key, int height); //创建一个新节点，高度是height
  int RandomHeight();//返回一个随机的高度值
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); } //判断2个key是否相等

  // Return true if key is greater than the data stored in "n"
  //key比n节点中的key要大
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return NULL if there is no such node.
  //
  // If prev is non-NULL, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  //返回skiplist中最大的节点
  Node* FindLast() const;

  // No copying allowed
  //禁止赋值和赋值
  SkipList(const SkipList&);
  void operator=(const SkipList&);
};

// Implementation details follow
template<typename Key, class Comparator>
//skiplist的内部节点
struct SkipList<Key,Comparator>::Node {
  explicit Node(const Key& k) : key(k) { }

  Key const key; //节点的key

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  //返回当前节点的第n层的节点
  Node* Next(int n) {
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return reinterpret_cast<Node*>(next_[n].Acquire_Load());
  }
  //设置当前节点的第n层的节点
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].Release_Store(x);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return reinterpret_cast<Node*>(next_[n].NoBarrier_Load());
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].NoBarrier_Store(x);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  port::AtomicPointer next_[1]; //存储当前节点的指针，next_[0]指向第0层的节点，next_[1]指向第1层的节点，next_[2]指向第2层的节点
};

//分配一个高度为height的节点，需要的内存是sizeof(指针)*(height-1)+sizeof(Node)；因为next_[1]已经有1个指针了，所以要少分配一个指针
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::NewNode(const Key& key, int height) {
  char* mem = arena_->AllocateAligned(
      sizeof(Node) + sizeof(port::AtomicPointer) * (height - 1));
  return new (mem) Node(key);
}

template<typename Key, class Comparator>
//迭代器构造函数设置list和node_为NULL
inline SkipList<Key,Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = NULL;
}

//如果node_不为NULL那就是一个valid的节点
template<typename Key, class Comparator>
inline bool SkipList<Key,Comparator>::Iterator::Valid() const {
  return node_ != NULL;
}

//返回node_指向的key
template<typename Key, class Comparator>
inline const Key& SkipList<Key,Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

//返回node->Next(0)节点，就是当前迭代器的指向节点的下一个节点
//迭代器的Next方法很高效，时间O(1)，直接取下个节点的指针即可
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

//调用node_=FindlessThan(node_->key)，如果返回的是头节点那么设置为NULL表示not valid node
//迭代器的Prev方法不高效，需要时间O(logN）：即从高height往低height遍历一遍skiplist来找到合适的节点
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

//找到大于或等于target的节点，并放在迭代器的节点node_中
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, NULL);
}

//设置迭代器的node_为头节点的Next(0)
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

//调用FindLast，如果返回的是头节点，表示skiplist目前为空，设置为not valid node
template<typename Key, class Comparator>
inline void SkipList<Key,Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = NULL;
  }
}

//返回一个随机的高度，高度为2:1/4概率，高度3:1/16概率，高度4:1/64概率
template<typename Key, class Comparator>
int SkipList<Key,Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  //高度2的概率：1/4；高度3的概率：1/4*1/4；高度4的概率：1/4*1/4*1/4。。。每增加高度1概率就乘1/4...最大的高度为kMaxHeight
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

//判断key是不是在n节点的后面，返回n不为空并且n->key要大于key
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // NULL n is considered infinite
  return (n != NULL) && (compare_(n->key, key) < 0);
}

//找和key相等或比key大的最小的节点，prev记录小于等于key的previous节点
//函数整体过程就是从最大的高度遍历，找到第一个等于或大于key的节点，并将沿途（从大的height到小的height)的小于key的最大节点给记录到prev[height_index]中
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindGreaterOrEqual(const Key& key, Node** prev)
    const {
  Node* x = head_; //x是当前遍历到的节点
  int level = GetMaxHeight() - 1;//最大的遍历index height
  while (true) {
    Node* next = x->Next(level); //next指向x的下个节点
    //如果key比next节点要大，那么再看看next的下个节点，更新x=next，此时level保持不变
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
    //此时key要小于等于next指向的节点(例如key=11, next=13)，next的上一个节点是x(例如key=9)，记录下level的情况下
    //当前key的previous节点是x
      if (prev != NULL) prev[level] = x;
      //如果level是0，表示最底层了，next节点肯定就是大于等于key的最小节点了，此时直接返回相当于跳出循环
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        //否则，当前level处理完了，继续处理下个level
        level--;
      }
    }
  }
}

//找到小于key的最大节点
//从最大的height往下找，找到一个小于key的最大节点，时间O(logN)
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node*
SkipList<Key,Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level); //next是当前遍历节点，上一个是x
    //如果next是NULL或next的key大于等于key
    if (next == NULL || compare_(next->key, key) >= 0) {
      //如果到了第一层，那么要么就是到了skiplist的最后一个节点了(next==NULL)要么接下来的都大于key了，直接返回x满足要求
      if (level == 0) {
        return x;
      } else {
        //如果没到第一层，那么本层level的剩下节点都不行了，继续处理下一层
        // Switch to next list
        level--;
      }
    } else {
    //否则，如果next不为空并且next->key小于当前key，那么继续处理本层level的下一个节点，更新x=next
      x = next;
    }
  }
}

//找到skiplist的最后一个节点
//从高height往下遍历，每层level都遍历到最后一个节点然后接着处理下层节点直到第一层节点，时间O(logN)
template<typename Key, class Comparator>
typename SkipList<Key,Comparator>::Node* SkipList<Key,Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  //从上往下遍历
  while (true) {
    Node* next = x->Next(level);//当前节点是x,下个节点是next
    //如果next是空，表示本层level已处理完了
    if (next == NULL) {
      //如果level==0表示已经到第一层了，那么直接返回x
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        //否则继续处理下一层level
        level--;
      }
    } else {
     //否则本层level还有节点没遍历完，继续遍历下一个节点
      x = next;
    }
  }
}

template<typename Key, class Comparator>
SkipList<Key,Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)), //头节点是dummy节点，高度为最大高度
      max_height_(reinterpret_cast<void*>(1)), //默认高度为1，即只有1层
      rnd_(0xdeadbeef) {
  //可遍历的index是高度-1
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, NULL);//设置头节点的next都为空
  }
}

//插入key到skiplist中，要先生成一个随机高度，然后逐层进行插入
template<typename Key, class Comparator>
void SkipList<Key,Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.//通常都是外部调用时已经加了锁，所以这里不需要特殊的加锁处理了
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);//x记录大于等于key的节点，prev记录x的前一个节点

  // Our data structure does not allow duplicate insertion
  assert(x == NULL || !Equal(key, x->key));//断言要么x为NULL表示skiplist中节点的key都小于key，并且key和x->key不相等
  //即不能将已有的key插入skiplist中，否则就直接core了。。。

  int height = RandomHeight();//获得这个key对应节点的高度
  if (height > GetMaxHeight()) {
    //对于[max_height, height]，设置prev[i]=head_，方便下面的0...height的遍历
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    //fprintf(stderr, "Change height from %d to %d\n", max_height_, height);

    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (NULL), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since NULL sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.NoBarrier_Store(reinterpret_cast<void*>(height)); //更新当前skiplist的max_height
  }

  x = NewNode(key, height); //创建出一个节点出来，高度为height
  //从0...height，将x插入到每一层中（每一层的节点插入类似于单链表的插入）
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

//判断key是否在skiplist中，时间O(logN)
template<typename Key, class Comparator>
bool SkipList<Key,Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, NULL);//找打大于等于key的节点x
  //如果x节点不为空并且x.key和key相等的话，表示key存在于skiplist中，返回true
  if (x != NULL && Equal(key, x->key)) {
    return true;
  } else {
  //否则返回false
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
