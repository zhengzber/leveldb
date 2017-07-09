// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <stdio.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() {
}

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),//block的临时buffer
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {
}

Reader::~Reader() {
  delete[] backing_store_;//释放block的临时buffer
}

//跳到initial_offset所在的block的起始地址
bool Reader::SkipToInitialBlock() {
  /*
  假设initial_offset=32k+2，那么offset_in_block=2, block_start_location=32k
  */
  size_t offset_in_block = initial_offset_ % kBlockSize;
  uint64_t block_start_location = initial_offset_ - offset_in_block; //当前block的起始地址

  // Don't search a block if we'd be in the trailer
  //如果当前读取位置在当前block的trailer，那么跳过当前block，从下个block开始处理。
  if (offset_in_block > kBlockSize - 6) {
    offset_in_block = 0;
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}

//读取一条逻辑记录，底层调用ReadPhysicalRecord将多条物理记录结合起来。
//对于FullType来说的话record里面使用backing_store内存，而对于First/Middle/Last来说的话 里面使用的是scratch分配的内存
//读取一条完整的逻辑记录给用户，逻辑记录放在record中，scratch是用于拼接记录的第一部分、中间部分和最后部分数据用的
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  //如果上条记录的偏移量小于初始化读取偏移量，那么跳到第一个block处（即从initial_offset开始的往下找到的第一个block)
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    //尝试读一条记录，记录的用户数据赋值到fragment中，其实整个块的内存是在backing_store中
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    //当前记录的起始地址（调用ReadPhysicalRecord后，end_of_buffer_offset会加上读取到的整个block的大小，
    //而buffer会将kHeaderSize和fragment的大小给revmoePrefix，所以当前buffer.size+kHeaderSize+fragment.size的大小
    //就是ReadPhysicalRecord读取到的block的大小）
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }
        //当前记录的起始地址
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment; //读到一条完整的记录了，赋值给record
        last_record_offset_ = prospective_record_offset; //读到一个条完整的记录，会返回这条完整的记录,
        //last_record_offset会记录当前块的起始地址作为上条记录的起始地址
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (scratch->empty()) {
            in_fragmented_record = false;
          } else {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }
        //记录期望本次调研ReadRecord读到的记录的起始地址
        prospective_record_offset = physical_record_offset;
        //将读到的第一部分内容放入scratch中
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true; //当前读到了第一部分了，继续读下面的物理记录来拼接一个完整的逻辑记录
        break;

      case kMiddleType:
        //必须是in_gragmented_record，如果不是，那么之前读的是错的
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          //把读到的中间部分数据拼接到scratch中
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        //必须是in_fragmented_record，如果不是，那么之前读的是错的
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          //把最后一部分数据拼接到scratch中
          scratch->append(fragment.data(), fragment.size());
          //将scratch的数据完整赋值给record
          *record = Slice(*scratch);
          //对下条记录而言，当前这条记录的起始地址就是last_record_offset
          last_record_offset_ = prospective_record_offset;
          //返回一条完整的逻辑记录
          return true;
        }
        break;

      case kEof:
        //到文件结尾了，没啥完整的记录了，读取逻辑记录失败
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        //记录有问题，例如crc校验失败了，或者记录中的用户数据长度为0，返回失败
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() {
  return last_record_offset_;
}

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != NULL &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

//从文件里读取出一个kBlocksize大小的物理块出来，放在backingstore_里, 同时返回type
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {
    if (buffer_.size() < kHeaderSize) { //如果之前读取的内容不够kHeaderSize大小的话，第一次进入该函数也会进入到该逻辑中
      if (!eof_) { //如果上次读取没有到末尾的话，那么认为上次读取无效，直接忽略
        // Last read was a full read, so this is a trailer to skip
        buffer_.clear();
        //忽略之后，重新读取一个新块，新块内容是buffer_
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size(); //缓存块偏移指向这个块的结尾
        if (!status.ok()) { //如果读取失败
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof; //读取失败则返回结尾
        } else if (buffer_.size() < kBlockSize) { 
          //读取成功，但已经达到文件结尾了（因为要读取32k却没读到这么多，肯定是达到结尾了）
          //，重新判断读取的是否为正确的记录。如果没有读取正确的记录（<kHeaderSize），
          //那么会进入下面的错误逻辑，否则进入正常逻辑
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        buffer_.clear();
        return kEof;
      }
    }

    //走到这里的话，那么肯定buffer.size>kHeaderSize并且buffer.size>=kBlockSize，否则会一直continue回到循环的开始处的
    //即假设到这里的话，我们已经读取一个完整的block了，解析记录头
    // Parse the header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = header[6]; //记录类型
    const uint32_t length = a | (b << 8);//记录长度
    //如果记录头+数据长度大于buffer大小，那么读取到的记录肯定有异常
    if (kHeaderSize + length > buffer_.size()) { 
      size_t drop_size = buffer_.size();
      buffer_.clear();
      //把读到的异常数据给抛掉
      //如果还没到文件的结尾，那么读到一个异常记录
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      //否则已经到文件结尾了，可能情况是之前的writer写文件时只写header和部分数据，没有写完整
      return kEof;
    }

    //直接忽略数据长度为0的记录
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);//做crc校验，需要包含type字段
      if (actual_crc != expected_crc) { //校验失败，丢掉buffer
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        //crc校验失败，那么可能此时的length字段有异常，读到的用户数据不可信，整个block数据丢掉
        return kBadRecord;
      }
    }

    //buffer_是一块的长度，当读取结束一条记录时
    //buffer_指向内容的指针向前移动KheaderSize+length，即下一条记录的起始地址
    //此时buffer_指向下条记录的开始。
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    //跳过初始地址之前的记录
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    //将一条记录的用户数据赋值给result，即起始地址是header+7字节，长度是用户数据的长度
    *result = Slice(header + kHeaderSize, length);
    //返回获取到的这条记录的类型
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
