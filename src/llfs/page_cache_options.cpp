//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/page_cache_options.hpp>
//
#include <batteries/assert.hpp>
#include <batteries/bit_ops.hpp>
#include <batteries/math.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheOptions PageCacheOptions::with_default_values()
{
  PageCacheOptions opts;

  opts.default_log_size_ = 64 * kMiB;
  opts.set_byte_size(4 * kGiB);

  return opts;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheOptions& PageCacheOptions::add_sharded_view(PageSize page_size, PageSize shard_size)
{
  BATT_CHECK_LT(shard_size, page_size);
  BATT_CHECK_EQ(batt::bit_count(page_size.value()), 1);
  BATT_CHECK_EQ(batt::bit_count(shard_size.value()), 1);

  this->sharded_views.emplace(page_size, shard_size);
  return *this;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheOptions& PageCacheOptions::set_byte_size(usize max_size, Optional<PageSize> min_page_size)
{
  const usize bytes_per_slot = min_page_size.value_or(PageSize{kDirectIOBlockSize});

  this->max_cache_size_bytes = MaxCacheSizeBytes{max_size};
  this->cache_slot_count =
      SlotCount{(this->max_cache_size_bytes + bytes_per_slot - 1) / bytes_per_slot};

  return *this;
}

}  // namespace llfs
