//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_ALLOCATE_OPTIONS_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_cache_insert_options.hpp>

namespace llfs {

struct PageAllocateOptions : PageCacheInsertOptions {
  using Self = PageAllocateOptions;
  using Super = PageCacheInsertOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static const batt::CancelToken& no_cancel_token()
  {
    static const batt::CancelToken token_{None};
    return token_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  batt::WaitForResource wait_for_resource_ = batt::WaitForResource::kFalse;

  const batt::CancelToken* cancel_token_ = std::addressof(Self::no_cancel_token());

  //+++++++++++-+-+--+----- --- -- -  -  -   -
};

}  // namespace llfs
