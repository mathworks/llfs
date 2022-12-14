//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_TESTING_FAKE_PAGE_CACHE_HPP
#define LLFS_TESTING_FAKE_PAGE_CACHE_HPP

namespace llfs {

class FakePageArena
{
 public:
  MemoryLogDevice allocator_mem_log;
  FakeLogDeviceFactory<MemoryLogStorageDriver> allocator_log_factory;
  FakePageDevice fake_page_device;
};

class FakePageCache
{
 public:
  class Snapshot;

  explicit FakePageCache(const PageCacheOptions& options,
                         const std::vector<std::pair<PageCount, PageSize>>& arena_sizes);

  explicit FakePageCache(const PageCacheOptions& options, const Snapshot& snapshot);

  std::shared_ptr<PageCache> get_page_cache() const;

 private:
};

}  // namespace llfs

#endif  // LLFS_TESTING_FAKE_PAGE_CACHE_HPP
