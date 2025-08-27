//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_options.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheOptions PageCacheOptions::with_default_values()
{
  PageCacheOptions opts;

  opts.default_log_size_ = 64 * kMiB;
  opts.max_cache_size_bytes = MaxCacheSizeBytes{4 * kGiB};
  opts.cache_slot_count = SlotCount{opts.max_cache_size_bytes / (4 * kKiB)};

  return opts;
}

}  // namespace llfs
