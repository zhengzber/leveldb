// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

//将uint32_t的每个字节放入一个char即可
void EncodeFixed32(char* buf, uint32_t value) {
  if (port::kLittleEndian) { //小端直接复制字节即可
    memcpy(buf, &value, sizeof(value));
  } else {
    //如果是大端，则一个一个字节的复制
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
  }
}
  
//将uint64_t的每个字节放入一个char即可
void EncodeFixed64(char* buf, uint64_t value) {
  if (port::kLittleEndian) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = value & 0xff;
    buf[1] = (value >> 8) & 0xff;
    buf[2] = (value >> 16) & 0xff;
    buf[3] = (value >> 24) & 0xff;
    buf[4] = (value >> 32) & 0xff;
    buf[5] = (value >> 40) & 0xff;
    buf[6] = (value >> 48) & 0xff;
    buf[7] = (value >> 56) & 0xff;
  }
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)]; //声明一个4个元素的char buf[4]
  EncodeFixed32(buf, value); //将value的每个字节都放入char中
  dst->append(buf, sizeof(buf));//将buf append到dst的后面去，相当于把字节数据都append进去
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

//这种转型将一个字节分成两部分，前7个字节存储数据，第8个字节表示高位是否还有数据。
//一个字节(unsigned char)的低7位存储数值，第8位表示高位是否有数字了！如果第8位为1，表示高位仍然有数字应该继续解析，否则表示无数字了不用解析了。
char* EncodeVarint32(char* dst, uint32_t v) {
  // Operate on characters as unsigneds
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128; //二进制表示为1000 0000
  if (v < (1<<7)) {
    *(ptr++) = v; //如果v小于128，那么低7位赋值给ptr，ptr的第8位是0，ptr+1，即用1个字节来存储v即可
  } else if (v < (1<<14)) {
    *(ptr++) = v | B; //如果v大于等于128，小于1<<14，那么v|B的低7位是v的低7位，第8位会1表示高位仍然有数字。
    *(ptr++) = v>>7; //然后把v的高7位赋值给ptr+1的低7位，ptr+1的第8位是0，表示高位没数字了
  } else if (v < (1<<21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = v>>14;
  } else if (v < (1<<28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = v>>21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v>>7) | B;
    *(ptr++) = (v>>14) | B;
    *(ptr++) = (v>>21) | B;
    *(ptr++) = v>>28;
  }
  return reinterpret_cast<char*>(ptr);
}

//将v按照varint32来编码放入char buf[]中，再将buf append到dst中
void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5];
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf); //ptr-buf表示用于encode v花去了几个字节。
}

//将uint64_t v编码到dst中
char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  //因为uint64_t的值可能非常大，不能每次都移位判断是否大于128啥的
  while (v >= B) {
    *(ptr++) = (v & (B-1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char*>(ptr);
}

//将uint64_t v按照varint编码后，append到dst的尾部
void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

  
//先将value的大小用varint32来编码然后放入dst中，然后将value的数据即.data放入dst中。这样dst就包含字符串数据的大小和地址了。
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

//用varint来编码uint64_t需要的字节长度，即需要的char buf[]数组的大小
int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

//将char* [p...limit]中的字节按照varint的格式解析成uint32_t，然后放入value中
const char* GetVarint32PtrFallback(const char* p,
                                   const char* limit,
                                   uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const unsigned char*>(p)); //取出这个字节
    p++;
    if (byte & 128) { //表示高位仍然有数字
      // More bytes are present
      result |= ((byte & 127) << shift); //将低7位赋值给result
    } else {
      //此时高位没有数字了，把最后的7位赋值给result，然后返回
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

//尝试将input的数据解析成uint32_t赋值给value，剩下的部分仍然留给input
bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint32Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q); //剩下的字节仍然赋值会input
    return true;
  }
}

//将char* [p...limit]的字节解析成uint64_t，然后赋值给value，返回char*
const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const unsigned char*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return NULL;
}

//尝试将input的数据解析成uint64_t赋值给value，剩下的部分仍然留给input
bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == NULL) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

//p...limit之间先放了字节的长度，然后放了字节数据。这个函数从p...limit之间先解析出了字节长度uint32_t，然后按照长度把字节数据赋值给result
const char* GetLengthPrefixedSlice(const char* p, const char* limit,
                                   Slice* result) {
  uint32_t len;
  //先将p, limit的部分把uint32_t解析出来，作为len
  p = GetVarint32Ptr(p, limit, &len);
  if (p == NULL) return NULL;
  if (p + len > limit) return NULL;
  //然后剩下的字节赋值给result
  *result = Slice(p, len);
  return p + len;
}

//input里存了长度和数据，先把长度解析出来，然后按照长度把数据拿出来赋值给result。
bool GetLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len;
  if (GetVarint32(input, &len) &&
      input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb
