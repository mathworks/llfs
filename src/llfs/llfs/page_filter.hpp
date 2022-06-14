#pragma once
#ifndef LLFS_PAGE_FILTER_HPP
#define LLFS_PAGE_FILTER_HPP

#include <llfs/bloom_filter.hpp>
#include <llfs/key.hpp>
#include <llfs/page_device.hpp>
#include <llfs/page_filter_policy.hpp>
#include <llfs/slice.hpp>

namespace llfs {

class PageFilter
{
 public:
  PageFilter(const PageFilter&) = delete;
  PageFilter& operator=(const PageFilter&) = delete;

  virtual ~PageFilter() = default;

  PageId page_id() const
  {
    return this->page_id_;
  }

  virtual bool might_contain_key(const KeyView& key) = 0;

 protected:
  explicit PageFilter(PageId page_id) noexcept : page_id_{page_id} {};

 private:
  PageId page_id_;
};

// The null filter: always says a page might contain a given key (i.e. always returns true).
//
class NullPageFilter : public PageFilter
{
 public:
  explicit NullPageFilter(PageId page_id) noexcept : PageFilter{page_id}
  {
  }

  bool might_contain_key(const KeyView& key) override;
};

// PageBloomFilter: store a bloom filter with fixed bit rate for the page.
//
class PageBloomFilter : public PageFilter
{
 public:
  template <typename GetKeyItems>
  static std::shared_ptr<PageBloomFilter> build(const BloomFilterParams& params, PageId page_id,
                                                const GetKeyItems& items);

  explicit PageBloomFilter(PageId page_id, std::unique_ptr<u64[]> memory) noexcept;

  bool might_contain_key(const KeyView& key) override;

 private:
  std::unique_ptr<u64[]> memory_;
  PackedBloomFilter* filter_;
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

template <typename GetKeyItems>
/*static*/ inline std::shared_ptr<PageBloomFilter> PageBloomFilter::build(
    const BloomFilterParams& params, PageId page_id, const GetKeyItems& items)
{
  const usize size = packed_sizeof_bloom_filter(params, items.size());
  const usize word_size = (size + sizeof(u64) - 1) / sizeof(u64);
  BATT_CHECK_EQ(word_size * sizeof(u64), size);
  std::unique_ptr<u64[]> memory{new u64[word_size]};
  auto filter = reinterpret_cast<PackedBloomFilter*>(memory.get());

  filter->initialize(params, items.size());

  parallel_build_bloom_filter(batt::WorkerPool::default_pool(), std::begin(items), std::end(items),
                              /*hash_fn=*/BATT_OVERLOADS_OF(get_key), filter);

  return std::make_shared<PageBloomFilter>(page_id, std::move(memory));
}

}  // namespace llfs

#endif  // LLFS_PAGE_FILTER_HPP
