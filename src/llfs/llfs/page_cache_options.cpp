#include <llfs/page_cache_options.hpp>
//

namespace llfs {
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageCacheOptions PageCacheOptions::with_default_values()
{
  PageCacheOptions opts;

  opts.max_refs_per_page_ = 1 * kMiB;
  opts.default_log_size_ = 64 * kMiB;
  opts.max_cached_pages_per_size_log2.fill(0);

  // Assume that 512..8192 are node sizes; allow a million nodes to be cached.
  //
  for (usize n = 9; n <= 13; ++n) {
    opts.max_cached_pages_per_size_log2[n] = 1 * kMiB;
  }

  // Assume 16384..4Bil are leaf sizes; allow a thousand such pages to be cached.
  //
  for (usize n = 14; n < kMaxPageSizeLog2; ++n) {
    opts.max_cached_pages_per_size_log2[n] = 1 * kKiB;
  }

  return opts;
}
}  // namespace llfs
