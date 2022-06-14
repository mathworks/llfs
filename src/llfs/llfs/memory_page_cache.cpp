#include <llfs/memory_page_cache.hpp>
//

#include <llfs/memory_page_arena.hpp>

#include <batteries/stream_util.hpp>

#include <map>

namespace llfs {

batt::SharedPtr<PageCache> make_memory_page_cache(
    batt::TaskScheduler& scheduler, const std::vector<std::pair<PageCount, PageSize>>& arena_sizes,
    MaxRefsPerPage max_refs_per_page)
{
  std::unordered_map<PageSize, PageCount, PageSize::Hash> dedup;
  for (const auto& [count, size] : arena_sizes) {
    dedup[size] += count;
  }

  auto cache_options = PageCacheOptions::with_default_values()  //
                           .set_max_refs_per_page(max_refs_per_page);

  std::vector<PageArena> arenas;
  page_device_id_int device_id = 0;
  for (const auto& [size, count] : dedup) {
    arenas.emplace_back(make_memory_page_arena(
        scheduler, count, size, batt::to_string("Arena", device_id, "_", size), device_id));
    device_id += 1;
    cache_options.set_max_cached_pages_per_size(size, count);
  }

  return BATT_OK_RESULT_OR_PANIC(PageCache::make_shared(
      /*storage_pool=*/std::move(arenas),
      /*options=*/cache_options));
}

}  // namespace llfs
