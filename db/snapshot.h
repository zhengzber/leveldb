// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SNAPSHOT_H_
#define STORAGE_LEVELDB_DB_SNAPSHOT_H_

#include "db/dbformat.h"
#include "leveldb/db.h"

namespace leveldb {

class SnapshotList;

/*
Snapshot实现非常简单，就是一个双向链表的节点，然后挂在一个双向链表上面。 
每一个Snapshot实现都附带一个seq number.对于Snapshot最重要的应该是在上面的操作吧. 
我们可以猜想对于每次插入的key都会带上一个seq number.这样如果对snapshot操作的话读取的话，
那么只需要读取seq number以下的内容即可了。 
*/ 
 /*
 快照节点里仅仅包含一个sequence number，然后挂在快照的双向链表中，链表的dummpy head的prev节点是最新的
 next节点是最旧的，因为leveldb里每个key都有一个sequence number，可根据快照的sequence number查找出当时的key对应的value是啥
 
 leveldb的快照主要功能是用来读取某个时间点之前的数据，因为leveldb在插入数据时，键值是可以一样的，
 所以当查询这个键值时，系统返回的是最新的数据，
 也就是后面插入的数据。但是如果在第二次插入相同键值数据之前，建立一个快照，那么读取这个快照时，
 读取的就是这个快照时间点之前的数据。
 */
 
// Snapshots are kept in a doubly-linked list in the DB.
// Each SnapshotImpl corresponds to a particular sequence number.
//每个SnapshotImpl是一个节点
class SnapshotImpl : public Snapshot {
 public:
  SequenceNumber number_;  // const after creation 当前快照的序列号 
 
 private:
  friend class SnapshotList;

  // SnapshotImpl is kept in a doubly-linked circular list
 // 用于插入链表时，更新前后关系
  SnapshotImpl* prev_;
  SnapshotImpl* next_;

  SnapshotList* list_;                 // just for sanity checks //这个节点所属的链表，源码注释是“合理性检查”
};

//双向链表
//这个是SnapShot实现类也就是dbimpl中操作的快照类，每生成一个快照时，要插入双向链表中，
class SnapshotList {
 public:
  SnapshotList() {
   //初始dummy节点时，前后节点为自己
    list_.prev_ = &list_;
    list_.next_ = &list_;
  }

  bool empty() const { return list_.next_ == &list_; } //判断是否为空
  SnapshotImpl* oldest() const { assert(!empty()); return list_.next_; }//取出最“老”的快照
  SnapshotImpl* newest() const { assert(!empty()); return list_.prev_; }//取出最“新”的快照

  ////新生成一个快照，并插入链表中
  const SnapshotImpl* New(SequenceNumber seq) {
    SnapshotImpl* s = new SnapshotImpl;
    s->number_ = seq;
    s->list_ = this;
    s->next_ = &list_;
    s->prev_ = list_.prev_;
    s->prev_->next_ = s;
    s->next_->prev_ = s;
    return s;
  }

  ////删除一个快照
  void Delete(const SnapshotImpl* s) {
    assert(s->list_ == this);
    s->prev_->next_ = s->next_;
    s->next_->prev_ = s->prev_;
    delete s;
  }

 private:
  // Dummy head of doubly-linked list of snapshots
  SnapshotImpl list_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SNAPSHOT_H_
