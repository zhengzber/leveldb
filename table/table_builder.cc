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
  uint64_t offset; //当前sst文件的下个可写偏移量，记录data-block和其它Block的开始位置
  Status status; //file操作返回的status
  BlockBuilder data_block; //写data block的类
  BlockBuilder index_block; //写index block的类
  std::string last_key; //上次加入的key-value的key，用于和当前key来找comparator或当前没有key了用来找short successor
  int64_t num_entries;//已经加入的key-value数量
  bool closed;          // Either Finish() or Abandon() has been called. 文件写结束了
  FilterBlockBuilder* filter_block; //filter-block的builder

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;//当前加入的key是不是data-block的第一个key
  BlockHandle pending_handle;  // Handle to add to index block 待写入sst的data-block的offset和size

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
    //即index-block的key不存在前缀压缩
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

  //如果待加入的key是当前data-block的第一个key
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    //// 那么我们这里做一个index.
    // index key是按照last_ley和key之间的FindShortestSeparator得到的
    // 这样可以使用二分法来进行搜索
    
    //阅读完Finish会发现这里handle_encoding实际上是就是last_key的位置.
    // 这里使用FindShortestSeparator更加节省空间作为index_block里面的内容.:).
    // 但这里也决定了index_block的key不能够作为data block里面准确的key.
    
    //将上个data-block的位置加入index-block中，找到shortest separator作为分隔key
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
  r->data_block.Add(key, value);//data-block添加key-value
  
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
  // 第二个参数是出参，后面的函数会把当前data-block的offset和size记录到pending-handle中
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
//handle是出参，用于记录这个block写入到文件的offset和size。方便后面写。例如，data-block调用这个函数，
//那么handle会记录data-block的起始offset和size，然后offset和size作为index-block的value记录下来。
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
  handle->set_offset(r->offset);//将准备写入sst的data-block的offset记录到pending-handle中
  handle->set_size(block_contents.size());//将准备写入sst的data-block的size记录到pending-handle中
  r->status = r->file->Append(block_contents);//将block内容追加到文件中
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
  Flush();//将当前的data-block的数据追加到文件中
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;
  // filter_block_handle记录filter-block的offset和size
  // metaindex_block_handle记录metaindex-block的offset和size
  // index-block_handler记录index-block的offset和size

  //至此，data-block的内容都写入到了sstable中了，接下来开始写其他Block内容
  
  // 写filter block，调用filterblockbuilder的finish函数返回所有内容
  //将filter-block的offset和size记录到filter_block_handle中
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);//将这个meta block的偏移量和写进filter_block_handle，用于metaindex block写
  }

  // Write filter-index block
  // 写metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
       // meta_index block块内容格式为"filter.Name"
      // key是"filter+name"
      //value是BlockHandle encode的string，这里是filter-block的offset和size
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    //将meta-index-block的内容追加到文件，meta-index-block的offset和size记录在metaindex_block_handle中
    WriteBlock(&meta_index_block, &metaindex_block_handle); 
  }

  // 写index block
  if (ok()) {
    //如果有data-block追加到文件后，pending_index_entry=true
    if (r->pending_index_entry) {
      //记录最后一个追加的data-block的offset和size并放入index-block中
      r->options.comparator->FindShortSuccessor(&r->last_key);//因为没有新的key了，使用last_key的short successor作为分隔的最后的key
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);//encode最后一个data-block的offset和size
      r->index_block.Add(r->last_key, Slice(handle_encoding));//添加到index-block中
      r->pending_index_entry = false;
    }//写finish里flush函数刷新的数据块的偏移量和大小
    // 写入index block.
    //将index-block写入文件，并用index_block_handle来记录index-block的offset和size
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 写Footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);//记录metaindex-block的offset和size
    footer.set_index_handle(index_block_handle);//记录index-block的offset和size
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding); //将footer追加到文件中
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
