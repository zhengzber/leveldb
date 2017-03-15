// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

/*
*直方图的实现
*/

#ifndef STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
#define STORAGE_LEVELDB_UTIL_HISTOGRAM_H_

#include <string>

namespace leveldb {

class Histogram {
 public:
  Histogram() { }
  ~Histogram() { }

  void Clear();
  void Add(double value);
  void Merge(const Histogram& other);

  std::string ToString() const;

 private:
  double min_; //当前所有数据的最小值
  double max_; //当前所有数据的最大值
  double num_; //数据数量
  double sum_; //数量总和
  double sum_squares_;

  enum { kNumBuckets = 154 }; // bucket数量
  static const double kBucketLimit[kNumBuckets];
  double buckets_[kNumBuckets];

  double Median() const; //中位值
  double Percentile(double p) const; //百分比的数值，注意会在百分比数值所在的bucket做平滑
  double Average() const; //平均数
  double StandardDeviation() const;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_HISTOGRAM_H_
