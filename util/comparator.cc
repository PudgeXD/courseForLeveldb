// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <algorithm>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() { }

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() { }

  virtual const char* Name() const {
    return "leveldb.BytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const {
    return a.compare(b);
  }

  virtual void FindShortestSeparator(
      std::string* start,
      const Slice& limit) const {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // Do not shorten if one string is a prefix of the other
    } else {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  virtual void FindShortSuccessor(std::string* key) const {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i+1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};

class NumComparatorImpl : public Comparator {
public:
    NumComparatorImpl() { }

    virtual const char* Name() const {
      return "diy.NumComparator";
    }

    virtual int Compare(const Slice& a, const Slice& b) const {
        int int_a = atoi(a.ToString().c_str());
        int int_b = atoi(b.ToString().c_str());
        return int_a-int_b;
    }

    virtual void FindShortestSeparator(
            std::string* start,
            const Slice& limit) const {
        int int_start = atoi(start->c_str());
        int int_limit = atoi(limit.ToString().c_str());
        int sep = int_start+1;
        if(int_limit>sep)
        {
            *start = std::to_string(sep);
        }
    }

    virtual void FindShortSuccessor(std::string* key) const {
      // Find first character that can be incremented
      int int_key = atoi(key->c_str());
      *key = std::to_string(int_key+1);

      size_t n = key->size();
      for (size_t i = 0; i < n; i++) {
        const uint8_t byte = (*key)[i];
        if (byte != static_cast<uint8_t>(0xff)) {
          (*key)[i] = byte + 1;
          key->resize(i+1);
          return;
        }
      }
    }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

const Comparator* NumComparator() {
  static NoDestructor<NumComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
