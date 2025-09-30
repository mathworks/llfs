//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_BLOOM_FILTER_PAGE_HPP
#define LLFS_PACKED_BLOOM_FILTER_PAGE_HPP

#include <llfs/config.hpp>
//

#include <llfs/bloom_filter.hpp>
#include <llfs/buffer.hpp>
#include <llfs/get_page_const_payload.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_page_header.hpp>
#include <llfs/packed_page_id.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_layout_id.hpp>

#include <batteries/assert.hpp>
#include <batteries/bit_ops.hpp>

#include <xxhash.h>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
/** \brief Page layout struct for a packed Bloom filter.
 */
struct PackedBloomFilterPage {
  static constexpr u64 kMagic = 0xca6f49a0f3f8a4b0ull;

  /** \brief The page layout id for all instances of this class.
   */
  static PageLayoutId page_layout_id();

  /** \brief Returns the payload of the passed object (which may be a ConstBuffer, PageBuffer,
   * std::shared_ptr<PageBuffer>, PageView, or PinnedPage), cast to PackedBloomFilterPage.
   */
  template <typename T>
  static const PackedBloomFilterPage& view_of(T&& t) noexcept
  {
    const ConstBuffer buffer = get_page_const_payload(BATT_FORWARD(t));
    BATT_CHECK_GE(buffer.size(), sizeof(PackedBloomFilterPage));

    return *static_cast<const PackedBloomFilterPage*>(buffer.data());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief XXH3 hash of the bytes starting after this field and ending at the end of the Bloom
   * filter data; for data integrity checking.  Not calculated/verified by default (opt-in).
   */
  little_u64 xxh3_checksum;

  /** \brief Must be `PackedBloomFilterPage::kMagic`.
   */
  little_u64 magic;

  /** \brief The total number of 1 bits set in the Bloom filter.  Not calculated by default; can be
   * enabled for data integrity and/or debugging purposes.
   */
  little_u64 bit_count;

  /** \brief If this filter is associated with another page, the page id of that page is stored
   * here.
   */
  PackedPageId src_page_id;

  /** \brief The Bloom filter header; the actual filter bits follow this struct, and may extend to
   * the end of the page.
   */
  PackedBloomFilter bloom_filter;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Panic if the magic number is wrong.
   */
  void check_magic() const;

  /** \brief Returns non-ok status code if the magic number is wrong.
   */
  Status require_magic() const;

  /** \brief Computes the _true_ XXH3 hash of this page; can be used to populate or verify
   * `this->xxh3_checksum`.
   */
  u64 compute_xxh3_checksum() const;

  /** \brief Returns the number of 1 bits in the filter.
   */
  u64 compute_bit_count() const;

  /** \brief Runs integrity checks on this page; panics if anything doesn't check out.
   */
  void check_integrity() const;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Builds a PackedBloomFilterPage in the passed page buffer from the items in the passed
 * range.
 *
 * \param worker_pool A thread pool to use for parallel page building (or WorkerPool::null_pool())
 * \param items A random-access range of items to insert into the filter
 * \param key_from_item_fn A function which maps each element of `items` to either a string or u64
 * \param layout The Bloom filter layout to use (e.g. flat or blocked)
 * \param bits_per_key The number of bits per key
 * \param opt_hash_count (Optional) The number of hash functions to use; by default calculate the
 *         optimal value automatically
 * \param src_page_id (Optional) The page for which the returned page will act as a filter
 * \param compute_checksum Whether to compute full data integrity check information (expensive!)
 * \param dst_filter_page_buffer The buffer in which to build the filter; must be large enough for
 *         all headers and bits_per_key * size(items)
 */
template <typename ItemsRangeT, typename KeyFromItemFn>
StatusOr<const PackedBloomFilterPage*> build_bloom_filter_page(
    batt::WorkerPool& worker_pool,          //
    const ItemsRangeT& items,               //
    const KeyFromItemFn& key_from_item_fn,  //
    BloomFilterLayout layout,               //
    BitsPerKey bits_per_key,                //
    Optional<HashCount> opt_hash_count,     //
    PageId src_page_id,                     //
    ComputeChecksum compute_checksum,       //
    const std::shared_ptr<PageBuffer>& dst_filter_page_buffer)
{
  // Set the page layout in the destination page buffer.
  //
  PackedPageHeader* const filter_page_header = mutable_page_header(dst_filter_page_buffer.get());

  filter_page_header->layout_id = PackedBloomFilterPage::page_layout_id();

  // Pack the filter page header metadata.
  //
  MutableBuffer payload_buffer = dst_filter_page_buffer->mutable_payload();
  BATT_CHECK_EQ(filter_page_header->size, payload_buffer.size() + sizeof(PackedPageHeader));

  auto* const packed_filter_page = static_cast<PackedBloomFilterPage*>(payload_buffer.data());
  payload_buffer += sizeof(PackedBloomFilterPage);

  std::memset(packed_filter_page, 0, sizeof(PackedBloomFilterPage));

  packed_filter_page->xxh3_checksum = 0;
  packed_filter_page->magic = PackedBloomFilterPage::kMagic;
  packed_filter_page->bit_count = 0;
  packed_filter_page->src_page_id = PackedPageId::from(src_page_id);

  // Grab the item count to figure out how to size the filter.
  //
  const u64 item_count = std::distance(std::begin(items), std::end(items));

  // Calculate the total filter bits to use.
  //
  const u64 bits_per_word = 64;
  const u64 max_word_count = payload_buffer.size() / sizeof(little_u64);
  const u64 filter_page_capacity_in_bits = max_word_count * bits_per_word;
  const u64 filter_target_bits = item_count * bits_per_key;
  const u64 filter_size_in_bits =
      std::max<u64>(512, std::min(filter_page_capacity_in_bits, filter_target_bits));

  u64 word_count = batt::round_up_bits(/*log_2(512)=*/9, filter_size_in_bits) / bits_per_word;
  if (word_count * bits_per_word > filter_page_capacity_in_bits) {
    word_count -= 8 /*bytes == 64 bits*/;
    BATT_CHECK_LE(word_count * bits_per_word, filter_page_capacity_in_bits);
  }

  // Configure the filter.
  //
  auto config = BloomFilterConfig::from(layout, Word64Count{word_count}, ItemCount{item_count});
  BATT_CHECK_EQ(config.filter_size, word_count);

  if (opt_hash_count) {
    config.hash_count = *opt_hash_count;
  }

  // Initialize the filter header.
  //
  packed_filter_page->bloom_filter.initialize(config);

  // If the actual filter size is less than the page capacity, then don't write the whole page.
  //
  BATT_CHECK_EQ(packed_filter_page->bloom_filter.word_count(), word_count);
  const void* const filter_data_end = &packed_filter_page->bloom_filter.words[word_count];
  filter_page_header->unused_end = filter_page_header->size;
  filter_page_header->unused_begin = byte_distance(filter_page_header,  //
                                                   filter_data_end);

  BATT_CHECK_LE(filter_page_header->unused_begin, get_page_size(dst_filter_page_buffer));

  // Now the header is full initialized; build the full filter from the packed edits.
  //
  parallel_build_bloom_filter(worker_pool, std::begin(items), std::end(items), key_from_item_fn,
                              &packed_filter_page->bloom_filter);

  if (compute_checksum) {
    packed_filter_page->bit_count = packed_filter_page->compute_bit_count();
  }

  // IMPORTANT: must be set last.
  //
  if (compute_checksum) {
    packed_filter_page->xxh3_checksum = packed_filter_page->compute_xxh3_checksum();
  }

  BATT_CHECK_EQ(packed_filter_page->bloom_filter.layout(), layout);

  return packed_filter_page;
}

}  //namespace llfs

#endif  // LLFS_PACKED_BLOOM_FILTER_PAGE_HPP
