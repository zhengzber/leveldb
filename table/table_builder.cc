// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Options options; //上游传过来的options，是open的参数
  Options index_block_options; // index option
  WritableFile* file; // sst 文件封装类
  uint64_t offset; //当前向这个file写入了多少数据，sst文件的偏移量，每写入一个块后，更新这个变量
  Status status; //file操作返回的status
  BlockBuilder data_block; //写data block的类
  BlockBuilder index_block; //写index block的类
  std::string last_key; //上次插入的键值，用于写index block时最大键值
  int64_t num_entries;//已经插入了多少个kv
  bool closed;          // Either Finish() or Abandon() has been called. 文件写结束了
  FilterBlockBuilder* filter_block; //创建过滤器的类

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;//data block为空时，该值为true,用于写handler
  BlockHandle pending_handle;  // Handle to add to index block 用于写index block的handler

  std::string compressed_output; //作为compressed存放的内容.

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == NULL ? NULL
                     : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    //里面存放的key是全量，即用BlockBuilder来构造index block，只要把interval设置为1，即每个记录放的是完整的key和value
    //不存在共享key
    index_block_options.block_restart_interval = 1;
  }
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);//构造开始时，开始一个filter item
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  // // 需要确保comparator对象没有发生改变.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;//index block的restart_interval必须为1,，任何option都不能改变这个不变量
  return Status::OK();
}

//往data block添加一个kv
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  //// 确保按照顺序操作.如果已经插入了key，那么当前插入的key必须比之前插入的key要大
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  ////当data_block为空时，将上个datablock的handler添加到index block
  //如果这里新开辟一个block的话对于第一块没有.
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    //// 那么我们这里做一个index.
    // index key是按照last_ley和key之间的FindShortestSeparator得到的
    // 这样可以使用二分法来进行搜索
    
    //阅读完Finish会发现这里handle_encoding实际上是就是last_key的位置.
    // 这里使用FindShortestSeparator更加节省空间作为index_block里面的内容.:).
    // 但这里也决定了index_block的key不能够作为data block里面准确的key.
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);////将handle解码到字符串handle_encoding
    r->index_block.Add(r->last_key, Slice(handle_encoding));/////index_block添加一条记录
    r->pending_index_entry = false;//赋值为false，开始新一个数据块写
  }

  ////如果有定义过滤器，将这条记录键值添加到meta block的filter条目中
  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  // 更新last_key并且插入data block.
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;//记录数+1
  r->data_block.Add(key, value);//数据块添加记录
  
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  //如果当前data block的大小超过设定的值，那么刷新到磁盘
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  //将数据写回缓冲区
  // 将data block作为Block写入然后将这个handle放在pengding_handle里面.
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();//数据刷新到磁盘
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);//重新开启一个Filter条目
  }
}

//这个函数主要是用于判断data block的数据是否要压缩存储，真正下操作在下面函数： 
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  Slice raw = block->Finish();//data block原生内容

  Slice block_contents;
  CompressionType type = r->options.compression; //是否将数据压缩
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;//不压缩
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      // 尝试使用snappy compress.如果压缩超过12.5%的数据量的话，选择压缩后的数据。
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  //真正写操作
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();//重置data block，用于下次写
}

//这个函数主要作用就是将数据写进用户态缓冲区，添加类型和CRC码，更新偏移量。
void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type,
                                 BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);//设置当前块的偏移量，即设置index block的handle内容。
  handle->set_size(block_contents.size());//设置当前块的大小
  r->status = r->file->Append(block_contents);//将内容写进用户态缓冲区
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type; //追加type和crc
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer+1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    //写进一个块，这时sst文件偏移量增加，未增加前表示上个data_block的结束位置
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const {
  return rep_->status;
}

//sst写完成函数，用于上层调用
Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();//刷新最后的数据
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 写Meta block，调用filterblockbuilder的finish函数返回所有内容
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);//将这个meta block的偏移量和写进filter_block_handle，用于metaindex block写
  }

  // Write metaindex block
  // 写metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
       // meta_index block块内容格式为"filter.Name"
      // 实际上这个meta_index_block部分没有任何内容.
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle); // 写入之后然后得到handle.
  }

  // Write index block
  // 写index block
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }//写finish里flush函数刷新的数据块的偏移量和大小
    // 写入index block.
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 写Footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();//更新偏移量
    }
  }
  return r->status;
}

// 操作非常简单就是放弃构建.
void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const {
  return rep_->num_entries;
}

uint64_t TableBuilder::FileSize() const {
  return rep_->offset;
}

}  // namespace leveldb
