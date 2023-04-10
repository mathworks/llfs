//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_REF_COUNT_HPP
#define LLFS_PAGE_REF_COUNT_HPP

#include <llfs/int_types.hpp>
#include <llfs/page_id.hpp>

#include <boost/operators.hpp>

#include <limits>

namespace llfs {

struct PageRefCount {
  PageId page_id;
  i32 ref_count;

  struct Delta {
    // TODO [tastolfi 2023-03-31] Replace `page_id_int` with `PageId` here
    PageRefCount operator()(page_id_int page_id) const
    {
      return PageRefCount{PageId{page_id}, val_};
    }

    i32 val_;
  };
};

bool operator==(const PageRefCount& l, const PageRefCount& r);

usize hash_value(const PageRefCount& prc);

std::ostream& operator<<(std::ostream& out, const PageRefCount& t);

static_assert(i64{std::numeric_limits<i32>::max()} < -i64{std::numeric_limits<i32>::min()}, "");

constexpr i32 kRefCount_1_to_0 = std::numeric_limits<i32>::min();

static_assert(kRefCount_1_to_0 != 0, "");

}  // namespace llfs

namespace boost {
template struct equality_comparable<::llfs::PageRefCount>;
}

#endif  // LLFS_PAGE_REF_COUNT_HPP
