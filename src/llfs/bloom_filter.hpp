//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_BLOOM_FILTER_HPP
#define LLFS_BLOOM_FILTER_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/conversion.hpp>
#include <llfs/data_layout.hpp>
#include <llfs/filter_hash.hpp>
#include <llfs/key.hpp>
#include <llfs/seq.hpp>

#include <batteries/async/slice_work.hpp>
#include <batteries/async/work_context.hpp>
#include <batteries/async/worker_pool.hpp>
#include <batteries/math.hpp>

#include <batteries/hint.hpp>
#include <batteries/math.hpp>
#include <batteries/seq/loop_control.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/suppress.hpp>

#include <cmath>
#include <type_traits>

#ifdef __AVX512F__
#include <immintrin.h>
#endif

namespace llfs {

/** \brief Physical layout of a Bloom filter; each carries different trade-offs.
 */
enum struct BloomFilterLayout : u8 {

  /** \brief All keys are hashed/inserted into a single bit map.
   */
  kFlat = 0,

  /** \brief Filter is divided into flat filters (blocks) of 64 bits (8 bytes) each; a given key is
   * hashed/inserted into the block specified by its first hash function.  The bits set within that
   * block are determined by 6-bit indexes packed into the second, third, etc. hash function.
   */
  kBlocked64 = 1,

  /** \brief Same as kBlocked64, but uses 512 bit (64 byte) blocks.  This size is chosen to coincide
   * with common cache line sizes of the same number of bytes, so that lookups can be achieved in a
   * single cache line fetch (using bitwise SIMD instructions (AVX512) if available).
   */
  kBlocked512 = 2,
};

std::ostream& operator<<(std::ostream& out, BloomFilterLayout layout);

Word64Count min_filter_size(BloomFilterLayout layout) noexcept;

Word64Count block_size(BloomFilterLayout layout, Word64Count filter_size) noexcept;

usize block_count(BloomFilterLayout layout, Word64Count filter_size) noexcept;

/** \brief Parameters used to build a Bloom filter.
 */
struct BloomFilterConfig {
  using Self = BloomFilterConfig;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static HashCount hash_count_from_bits_per_key(RealBitCount bits_per_key)
  {
    static const double ln2 = std::log(2);
    return HashCount{(usize)std::ceil(bits_per_key * ln2)};
  }

  static constexpr BloomFilterLayout kDefaultLayout = BloomFilterLayout::kFlat;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  BloomFilterLayout layout;
  Word64Count filter_size;
  ItemCount key_count;
  HashCount hash_count;
  RealBitCount bits_per_key;
  FalsePositiveRate false_positive_rate;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self from(BloomFilterLayout layout,  //
                   ItemCount key_count,       //
                   FalsePositiveRate false_positive_rate) noexcept;

  static Self from(BloomFilterLayout layout,  //
                   ItemCount key_count,       //
                   RealBitCount bits_per_key) noexcept;

  static Self from(BloomFilterLayout layout,  //
                   Word64Count filter_size,   //
                   RealBitCount bits_per_key) noexcept;

  static Self from(BloomFilterLayout layout,  //
                   Word64Count filter_size,   //
                   HashCount hash_count) noexcept;

  static Self from(BloomFilterLayout layout,  //
                   Word64Count filter_size,   //
                   ItemCount key_count) noexcept;

  //----- --- -- -  -  -   -
  // Definitions:
  //  - m: filter_size_bits
  //  - n: key_count
  //  - k: (optimal) hash_count
  //  - b: bits_per_key
  //  - e: target_error_rate
  //  - p: keys per block
  //  - q: block count
  //  - s: block size (bits)
  //
  // p = n / q
  // m = q * s
  // q = m / s
  //
  // Bloom Filter Facts:
  //
  // (ln(x) = ln(2) * log2(x))
  // (ln(2) = ln(x) / log2(x))
  // (1/ln(2) =~ 1.44)
  // (1/ln(2)^2 =~ 2.08)
  // (ln(2) =~ 0.693)
  //
  //  - b = m / n                            (definition of bits per key)
  //  - b = k / ln(2)
  //  - b = -log2(e) / ln(2)                 (expected false positive rate)
  //
  //  - k = ln(2) * b                        (optimal number of hash functions)
  //  - k = ln(2) * (m / n)
  //  - k = -log2(e)
  //
  //  - e = 1 / 2^k
  //  - e = 1 / 2^(b * ln(2))
  //  - e = 1 / 2^(m * ln(2) / n)
  //
  //  - m =  n * b
  //  - m =  n * k / ln(2)
  //  - m = -n * log2(e) / ln(2)
  //  - n =  m / b
  //  - n =  m * ln(2) / k
  //  - n = -m * ln(2) / log2(e)
  //
  // Variable solutions:
  //
  //  - m
  //    - given
  //    - (n, k)
  //    - (n, b)
  //  - n
  //    - given
  //    - (m, b)
  //    - (m, e)
  //  - k
  //    - given
  //    - b
  //
  //
  //----- --- -- -  -  -   -

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Word64Count block_size() const noexcept;
  usize block_count() const noexcept;
  usize word_count() const noexcept;
};

std::ostream& operator<<(std::ostream& out, const BloomFilterConfig& t);

template <typename T>
struct BloomFilterQuery {
  T key;
  batt::SmallVec<u64, 24> hash;
  batt::SmallVec<u64, 8> mask;
  u32 mask_n_hashes = 0;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename Arg, typename = batt::EnableIfNoShadow<BloomFilterQuery, Arg&&>>
  explicit BloomFilterQuery(Arg&& key_arg) noexcept : key{BATT_FORWARD(key_arg)}
  {
    this->hash.push_back(get_nth_hash_for_filter(this->key, 0));
  }

  void update_cache(usize n_hashes)
  {
    usize i = this->hash.size();
    if (BATT_HINT_FALSE(i < n_hashes)) {
      do {
        this->hash.push_back(get_nth_hash_for_filter(this->key, i));
        ++i;
      } while (i != n_hashes);
    }
  }

  void update_mask(usize n_hashes, usize block_size)
  {
    switch (block_size) {
      case 1:
        this->update_mask_impl<1>(n_hashes);
        break;

      case 8:
        this->update_mask_impl<8>(n_hashes);
        break;

      default:
        BATT_PANIC() << "Invalid block size (words)=" << block_size;
        BATT_UNREACHABLE();
    }
  }

 private:
  template <usize kBlockWord64Size>
  void update_mask_impl(usize n_hashes) noexcept
  {
    // The number of bits in a 64-bit word.
    //
    constexpr usize kWord64BitSize = 64;

    // The number of bits needed to index any position within a 64-bit word.
    //
    constexpr usize kWord64IndexBits = batt::log2_ceil(kWord64BitSize);

    // The number of bits in a block.
    //
    constexpr usize kBlockBitSize = kBlockWord64Size * kWord64BitSize;

    // The number of bits needed to index any (bit) position within a block.
    //
    constexpr usize kBlockIndexBits = batt::log2_ceil(kBlockBitSize);

    // The number of bits needed to index any 64-bit word within a block.
    //
    constexpr usize kWord64InBlockIndexBits = kBlockIndexBits - kWord64IndexBits;

    // The number of block index values we can extract from a single 64-bit hash value.
    //
    constexpr usize kBlockIndicesPerWord64 = kWord64BitSize / kBlockIndexBits;

    // A bit-mask to extract an index to a 64-bit word within a block.
    //
    constexpr u64 kWord64InBlockIndexMask = (u64{1} << kWord64InBlockIndexBits) - 1;

    // A bit-mask to extract an index to a bit within a 64-bit word.
    //
    constexpr u64 kBitInWord64IndexMask = (u64{1} << kWord64IndexBits) - 1;

    // The `shift` value that signals running out of bits within a 64-bit hash value.
    //
    constexpr usize kShiftOverflow = kBlockIndexBits * (kBlockIndicesPerWord64 + 1);

    //----- --- -- -  -  -   -
    static_assert(kWord64InBlockIndexBits + kWord64IndexBits == kBlockIndexBits);
    //----- --- -- -  -  -   -

    const bool mask_size_changed = (this->mask.size() != kBlockWord64Size);

    // We must recalculate the mask if either the block size or number of hash functions changes.
    //
    if (BATT_HINT_FALSE(mask_size_changed || (this->mask_n_hashes != n_hashes))) {
      // Resize the hash value cache (this->hash) so we have enough bits for 1 64-bit index to
      // determine which block the key belongs to, plus enough index bits for `n_hashes` locations
      // within the block.
      //
      this->update_cache(((n_hashes + kBlockIndicesPerWord64 - 1) / kBlockIndicesPerWord64) + 1);

      // The mask should exactly as large as the block; all 0's initially.
      //
      this->mask.resize(kBlockWord64Size, 0);

      // Each time through the loop, we pull enough bits from this->hash[1..] to locate a single bit
      // within the mask.
      //
      // `src_val_j` is the current index into this->hash from which we are pulling bits, and
      // `shift` is our current position within this->hash[src_val_j].  `shift` is updated each time
      // though the loop; `src_val_j` is updated when `shift` reaches the "overflow" value.
      //
      for (usize hash_i = 0, src_val_j = 1, shift = 0; hash_i < n_hashes; ++hash_i) {
        BATT_ASSERT_LT(src_val_j, this->hash.size());

        // Extract bits to tell us which 64-bit word within our mask to set.
        //
        const usize word_k = (this->hash[src_val_j] >> shift) & kWord64InBlockIndexMask;

        BATT_ASSERT_LT(word_k, kBlockWord64Size);

        // Extract more bits to select a bit within `word_k`.
        //
        const usize bit_l =
            (this->hash[src_val_j] >> (shift + kWord64InBlockIndexBits)) & kBitInWord64IndexMask;

        BATT_ASSERT_LT(bit_l, kWord64BitSize);

        // Set the selected bit within the selected word inside the mask.
        //
        this->mask[word_k] |= (u64{1} << bit_l);

        // Move past the extracted bits and move to the next src if necessary.
        //
        shift += kBlockIndexBits;
        if (shift == kShiftOverflow) {
          shift = 0;
          ++src_val_j;
        }
      }

      // Finally store the new number of hashes.
      //
      this->mask_n_hashes = n_hashes;
    }
  }
};

template <typename T>
inline bool operator==(const BloomFilterQuery<T>& l, const BloomFilterQuery<T>& r) noexcept
{
  return l.key == r.key && l.hash == r.hash && l.mask == r.mask &&
         l.mask_n_hashes == r.mask_n_hashes;
}

template <typename T>
inline bool operator!=(const BloomFilterQuery<T>& l, const BloomFilterQuery<T>& r) noexcept
{
  return !(l == r);
}

template <typename T>
inline std::ostream& operator<<(std::ostream& out, const BloomFilterQuery<T>& t)
{
  return out << "BloomFilterQuery{.key=" << batt::make_printable(t.key)
             << ", .hash=" << batt::dump_range(t.hash) << ", .mask=" << batt::dump_range(t.mask)
             << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

struct PackedBloomFilter {
  using Self = PackedBloomFilter;

  /** \brief All items are inserted into a flat address space.
   */
  static constexpr u8 kLayoutFlat = static_cast<u8>(BloomFilterLayout::kFlat);

  /** \brief Use blocks of size 8 bytes (64 bits) for item addressing.
   */
  static constexpr u8 kLayoutBlocked64 = static_cast<u8>(BloomFilterLayout::kBlocked64);

  /** \brief Use blocks of size 64 bytes (512 bits) for item addressing.
   */
  static constexpr u8 kLayoutBlocked512 = static_cast<u8>(BloomFilterLayout::kBlocked512);

  // The size of the filter in 64-bit words.
  //
  // If using a blocked layout, then the block size can be derived by dividing this number by the
  // words-per-block.
  //
  little_u64 word_count_;

  // The number of blocks in this filter (1 == flat layout).
  //
  little_u64 block_count_;

  // The number of hash functions used.
  //
  little_u16 hash_count_;

  // The layout for this filter.
  //
  little_u8 layout_;

  // The ceiling of log2(word_count).
  //
  little_u8 word_count_pre_mul_shift_;

  // 64 - this->word_count_log2_ceil_.
  //
  little_u8 word_count_post_mul_shift_;

  // The ceiling of log2(block_count).
  //
  little_u8 block_count_pre_mul_shift_;

  // 64 - this->block_count_log2_ceil_.
  //
  little_u8 block_count_post_mul_shift_;

  // Align to 64-bit boundary.
  //
  little_u8 reserved_[41];

  // TODO [tastolfi 2025-02-01] - make sure words is cache-line aligned (esp. for blocked512
  // layout!)

  // The actual filter array starts here (it will probably be larger than one element...)
  //
  little_u64 words[0];

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  void initialize(const BloomFilterConfig& config) noexcept;

  void check_invariants() const noexcept;

  BloomFilterLayout layout() const noexcept
  {
    return static_cast<BloomFilterLayout>(this->layout_.value());
  }

  usize hash_count() const noexcept
  {
    return this->hash_count_;
  }

  /** \brief Returns the block size (in 64-bit words).
   */
  u64 block_size() const noexcept
  {
    return ::llfs::block_size(this->layout(), Word64Count{this->word_count()});
  }

  /** \brief Returns the number of blocks.
   */
  u64 block_count() const noexcept
  {
    return this->block_count_;
  }

  u64 word_index_from_hash(u64 hash_val) const
  {
    const u64 ans = ((hash_val >> this->word_count_pre_mul_shift_) * this->word_count_) >>
                    this->word_count_post_mul_shift_;

    // TODO [tastolfi 2025-02-01] Add unit test.
    //
    BATT_CHECK_LT(ans, this->word_count_)
        << BATT_INSPECT((i32)this->word_count_pre_mul_shift_) << std::endl
        << BATT_INSPECT((i32)this->word_count_post_mul_shift_) << std::endl
        << BATT_INSPECT(this->word_count_) << std::endl
        << [&](std::ostream& out) {
             std::bitset<64> a{hash_val};
             std::bitset<64> b{hash_val >> this->word_count_pre_mul_shift_};
             std::bitset<64> c{(hash_val >> this->word_count_pre_mul_shift_) * this->word_count_};
             std::bitset<64> d{ans};
             out << BATT_INSPECT(a) << std::endl
                 << BATT_INSPECT(b) << std::endl
                 << BATT_INSPECT(c) << std::endl
                 << BATT_INSPECT(d) << std::endl;
           };

    return ans;
  }

  u64 block_index_from_hash(u64 hash_val) const
  {
    const u64 ans = ((hash_val >> this->block_count_pre_mul_shift_) * this->block_count_) >>
                    this->block_count_post_mul_shift_;

    // TODO [tastolfi 2025-02-01] Add unit test.
    //
    BATT_CHECK_LT(ans, this->block_count_)
        << BATT_INSPECT((i32)this->block_count_pre_mul_shift_) << std::endl
        << BATT_INSPECT((i32)this->block_count_post_mul_shift_) << std::endl
        << BATT_INSPECT(this->word_count_) << std::endl
        << BATT_INSPECT(this->block_count_) << std::endl
        << [&](std::ostream& out) {
             std::bitset<64> a{hash_val};
             std::bitset<64> b{hash_val >> this->block_count_pre_mul_shift_};
             std::bitset<64> c{(hash_val >> this->block_count_pre_mul_shift_) * this->block_count_};
             std::bitset<64> d{ans};
             out << BATT_INSPECT(a) << std::endl
                 << BATT_INSPECT(b) << std::endl
                 << BATT_INSPECT(c) << std::endl
                 << BATT_INSPECT(d) << std::endl;
           };

    return ans;
  }

  static constexpr u64 bit_mask_from_hash(u64 hash_val)
  {
    return u64{1} << (hash_val & 63);
  }

  bool test_hash_value_flat(u64 h) const noexcept
  {
    return (this->words[this->word_index_from_hash(h)].value() & this->bit_mask_from_hash(h)) != 0;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  bool query_flat(BloomFilterQuery<T>& query) const noexcept
  {
    const usize n_hashes = this->hash_count();

    // First cache any hash values that aren't already cached.
    //
    query.update_cache(n_hashes);

    // Now test each hash against the bitmap.
    //
    for (usize i = 0; i < n_hashes; ++i) {
      if (!this->test_hash_value_flat(query.hash[i])) {
        return false;
      }
    }
    return true;
  }

  template <typename T>
  bool query_blocked64(BloomFilterQuery<T>& query) const noexcept
  {
    const usize n_hashes = this->hash_count();

    query.update_mask(n_hashes, /*block_size=*/1);

    const usize block_i = this->block_index_from_hash(query.hash[0]);

    return (this->words[block_i] & query.mask[0]) == query.mask[0];
  }

  template <typename T>
  bool query_blocked512(BloomFilterQuery<T>& query) const noexcept
  {
    const usize n_hashes = this->hash_count();

    query.update_mask(n_hashes, /*block_size=*/8);

    const usize block_i = this->block_index_from_hash(query.hash[0]);
    const usize block_first_word_i = block_i * 8;

    const i64* const block_p = (const i64*)&this->words[block_first_word_i];
    const i64* const query_p = (const i64*)&query.mask[0];

    BATT_CHECK_EQ(query.mask.size(), 8);

#ifdef __AVX512F__

    __m512i block_512 = {block_p[0], block_p[1], block_p[2], block_p[3],
                         block_p[4], block_p[5], block_p[6], block_p[7]};

    __m512i query_512 = {query_p[0], query_p[1], query_p[2], query_p[3],
                         query_p[4], query_p[5], query_p[6], query_p[7]};

    const bool match = _mm512_cmp_epi64_mask(_mm512_and_epi64(block_512, query_512), query_512,
                                             _MM_CMPINT_EQ) == __mmask8{0b11111111};

    return match;

#else

    return ((block_p[0] & query_p[0]) == query_p[0]) &&  //
           ((block_p[1] & query_p[1]) == query_p[1]) &&  //;
           ((block_p[2] & query_p[2]) == query_p[2]) &&  //;
           ((block_p[3] & query_p[3]) == query_p[3]) &&  //;
           ((block_p[4] & query_p[4]) == query_p[4]) &&  //;
           ((block_p[5] & query_p[5]) == query_p[5]) &&  //;
           ((block_p[6] & query_p[6]) == query_p[6]) &&  //;
           ((block_p[7] & query_p[7]) == query_p[7]);

#endif
  }

  template <typename T>
  bool query_blocked512_precached(BloomFilterQuery<T>& query) const noexcept
  {
    const usize block_i = this->block_index_from_hash(query.hash[0]);
    const usize block_first_word_i = block_i * 8;

    const i64* const block_p = (const i64*)&this->words[block_first_word_i];
    const i64* const query_p = (const i64*)query.mask.data();

#ifdef __AVX512F__

    __m512i block_512 = {block_p[0], block_p[1], block_p[2], block_p[3],
                         block_p[4], block_p[5], block_p[6], block_p[7]};

    __m512i query_512 = {query_p[0], query_p[1], query_p[2], query_p[3],
                         query_p[4], query_p[5], query_p[6], query_p[7]};

    const bool match = _mm512_cmp_epi64_mask(_mm512_and_epi64(block_512, query_512), query_512,
                                             _MM_CMPINT_EQ) == __mmask8{0b11111111};

    return match;

#else

    return ((block_p[0] & query_p[0]) == query_p[0]) &&  //
           ((block_p[1] & query_p[1]) == query_p[1]) &&  //;
           ((block_p[2] & query_p[2]) == query_p[2]) &&  //;
           ((block_p[3] & query_p[3]) == query_p[3]) &&  //;
           ((block_p[4] & query_p[4]) == query_p[4]) &&  //;
           ((block_p[5] & query_p[5]) == query_p[5]) &&  //;
           ((block_p[6] & query_p[6]) == query_p[6]) &&  //;
           ((block_p[7] & query_p[7]) == query_p[7]);

#endif
  }

  template <typename T>
  bool query(BloomFilterQuery<T>& query) const noexcept
  {
    switch (this->layout()) {
      case BloomFilterLayout::kFlat:
        return this->query_flat(query);

      case BloomFilterLayout::kBlocked64:
        return this->query_blocked64(query);

      case BloomFilterLayout::kBlocked512:
        return this->query_blocked512(query);

      default:
        break;
    }
    BATT_PANIC() << "Invalid layout=" << this->layout();
    BATT_UNREACHABLE();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  bool might_contain(const T& item) const
  {
    BloomFilterQuery<const T&> query{item};

    switch (this->layout()) {
      case BloomFilterLayout::kFlat:
        return this->query_flat(query);

      case BloomFilterLayout::kBlocked64:
        return this->query_blocked64(query);

      case BloomFilterLayout::kBlocked512:
        return this->query_blocked512(query);

      default:
        break;
    }
    BATT_PANIC() << "Invalid layout=" << this->layout();
    BATT_UNREACHABLE();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  void insert_flat(const T& item) noexcept
  {
    for (usize hash_i = 0; hash_i < this->hash_count(); ++hash_i) {
      const u64 h = get_nth_hash_for_filter(item, hash_i);
      this->words[this->word_index_from_hash(h)] |= this->bit_mask_from_hash(h);
    }
  }

  template <typename T>
  void insert_blocked64(const T& item) noexcept
  {
    BloomFilterQuery<const T&> query{item};
    query.update_mask(this->hash_count(), /*block_size=*/1);

    const usize block_i = this->block_index_from_hash(query.hash[0]);

    this->words[block_i] |= query.mask[0];
  }

  template <typename T>
  void insert_blocked512(const T& item) noexcept
  {
    BloomFilterQuery<const T&> query{item};
    query.update_mask(this->hash_count(), /*block_size=*/8);

    const usize block_i = this->block_index_from_hash(query.hash[0]);
    const usize block_first_word_i = block_i * 8;

    BATT_CHECK_LE(block_first_word_i + 8, this->word_count());
    BATT_CHECK_EQ(query.mask.size(), 8);

    i64* const block_p = (i64*)&this->words[block_first_word_i];
    const i64* const mask_p = (const i64*)&query.mask[0];

    block_p[0] |= mask_p[0];
    block_p[1] |= mask_p[1];
    block_p[2] |= mask_p[2];
    block_p[3] |= mask_p[3];
    block_p[4] |= mask_p[4];
    block_p[5] |= mask_p[5];
    block_p[6] |= mask_p[6];
    block_p[7] |= mask_p[7];
  }

  template <typename T>
  void insert(const T& item) noexcept
  {
    switch (this->layout()) {
      case BloomFilterLayout::kFlat:
        this->insert_flat(item);
        return;

      case BloomFilterLayout::kBlocked64:
        this->insert_blocked64(item);
        return;

      case BloomFilterLayout::kBlocked512:
        this->insert_blocked512(item);
        return;

      default:
        break;
    }
    BATT_PANIC() << "Invalid layout=" << this->layout();
    BATT_UNREACHABLE();
  }

  void clear()
  {
    std::memset(this->words, 0, sizeof(little_u64) * this->word_count());
  }

  usize word_count() const
  {
    return this->word_count_;
  }

  const void* filter_data_end() const
  {
    return std::addressof(this->words[this->word_count()]);
  }

  batt::Slice<const little_u64> get_words() const
  {
    return batt::as_slice(this->words, this->word_count());
  }

  auto dump() const noexcept
  {
    return [this](std::ostream& out) {
      usize offset = 0;
      for (const little_u64& w : this->get_words()) {
        out << std::endl
            << batt::to_string("[0x", std::hex, std::setw(8), std::setfill('0'), offset)
            << "]: " << std::bitset<64>{w.value()};
        offset += sizeof(little_u64);
        if ((offset & 63) == 0) {
          out << std::endl << std::string(75, '~');
        } else if ((offset & 31) == 0) {
          out << std::endl;
        }
      }
    };
  }
};

namespace {
BATT_STATIC_ASSERT_EQ(sizeof(PackedBloomFilter), 64);
}  // namespace

inline usize packed_sizeof(const PackedBloomFilter& filter) noexcept
{
  return sizeof(PackedBloomFilter) + sizeof(little_u64) * filter.word_count();
}

inline usize packed_sizeof_bloom_filter(const BloomFilterConfig& config) noexcept
{
  return sizeof(PackedBloomFilter) + to_bytes(config.filter_size);
}

template <typename Iter, typename GetKeyFn>
void parallel_build_bloom_filter(batt::WorkerPool& worker_pool, Iter first, Iter last,
                                 const GetKeyFn& get_key_fn, PackedBloomFilter* filter)
{
  if (worker_pool.size() == 0) {
    auto src = boost::make_iterator_range(first, last);
    filter->clear();
    for (const auto& item : src) {
      filter->insert(get_key_fn(item));
    }
    return;
  }

  const batt::WorkSliceParams stage1_params{
      .min_task_size =
          batt::TaskSize{u64(1024 /*?*/ + filter->hash_count() - 1) / filter->hash_count()},
      .max_tasks = batt::TaskCount{worker_pool.size() + 1},
  };

  const batt::WorkSlicePlan stage1_plan{stage1_params, first, last};

  const batt::InputSize item_count = stage1_plan.input_size;

  BATT_SUPPRESS_IF_GCC("-Wtype-limits")  // Tell GCC we don't care if this is always true...
  BATT_CHECK_GE(item_count, 0);
  BATT_UNSUPPRESS_IF_GCC()

  const batt::TaskCount n_input_shards = stage1_plan.n_tasks;

  using AlignedUnit = std::aligned_storage_t<64, 64>;

  const usize filter_size = packed_sizeof(*filter);
  const usize filter_unit_size = (filter_size + sizeof(AlignedUnit) - 1) / sizeof(AlignedUnit);

  BATT_CHECK_EQ(filter_size, sizeof(PackedBloomFilter) + filter->word_count() * sizeof(u64));

  std::unique_ptr<AlignedUnit[]> temp_memory{new AlignedUnit[filter_unit_size * n_input_shards]};
  batt::SmallVec<PackedBloomFilter*, 32> temp_filters;
  {
    AlignedUnit* ptr = temp_memory.get();
    for (usize i = 0; i < n_input_shards; ++i, ptr += filter_unit_size) {
      auto* partial = reinterpret_cast<PackedBloomFilter*>(ptr);
      *partial = *filter;
      temp_filters.emplace_back(partial);
    }
  }

  // Generate the filters for all sliced shards of the input, in parallel.
  {
    batt::ScopedWorkContext work_context{worker_pool};

    BATT_CHECK_OK(slice_work(work_context, stage1_plan,
                             /*gen_work_fn=*/
                             [&](usize task_index, isize task_offset, isize task_size) {
                               auto src_begin = std::next(first, task_offset);
                               return [src = boost::make_iterator_range(
                                           src_begin, std::next(src_begin, task_size)),
                                       dst = temp_filters[task_index], get_key_fn] {
                                 dst->clear();
                                 for (const auto& item : src) {
                                   dst->insert(get_key_fn(item));
                                 }
                               };
                             }))
        << "work_context must not be closed!";
  }

  // Merge the temporary filters by sliced output shard, in parallel.
  {
    const batt::WorkSliceParams stage2_params{
        .min_task_size = batt::TaskSize{(1024 /*?*/ + n_input_shards - 1) / n_input_shards},
        .max_tasks = batt::TaskCount{worker_pool.size() + 1},
    };

    const batt::WorkSlicePlan stage2_plan{stage2_params, batt::InputSize{filter->word_count()}};

    batt::ScopedWorkContext work_context{worker_pool};

    BATT_CHECK_OK(slice_work(work_context, stage2_plan,
                             /*gen_work_fn=*/
                             [&](usize /*task_index*/, isize task_offset, isize task_size) {
                               return [task_offset, task_size, filter, &temp_filters] {
                                 for (isize i = task_offset; i < task_offset + task_size; ++i) {
                                   filter->words[i] = 0;
                                   for (PackedBloomFilter* partial : temp_filters) {
                                     filter->words[i] |= partial->words[i];
                                   }
                                 }
                               };
                             }))
        << "work_context must not be closed!";
  }
}

}  // namespace llfs

#endif  // LLFS_BLOOM_FILTER_HPP
