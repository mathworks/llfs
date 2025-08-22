//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/bloom_filter.hpp>
//
#include <llfs/conversion.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, BloomFilterLayout layout)
{
  switch (layout) {
    case BloomFilterLayout::kFlat:
      return out << "Flat";

    case BloomFilterLayout::kBlocked64:
      return out << "Blocked64";

    case BloomFilterLayout::kBlocked512:
      return out << "Blocked512";

    default:
      break;
  }

  return out << "<INVALID:" << (i32)layout << ">";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Word64Count min_filter_size(BloomFilterLayout layout) noexcept
{
  switch (layout) {
    case BloomFilterLayout::kFlat:
      return Word64Count{1};

    case BloomFilterLayout::kBlocked64:
      return to_word64(BitCount{64});

    case BloomFilterLayout::kBlocked512:
      return to_word64(BitCount{512});

    default:
      break;
  }
  BATT_PANIC() << "Invalid layout: " << (i32)layout;
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Word64Count block_size(BloomFilterLayout layout, Word64Count filter_size) noexcept
{
  switch (layout) {
    case BloomFilterLayout::kFlat:
      return filter_size;

    case BloomFilterLayout::kBlocked64:
      return to_word64(BitCount{64});

    case BloomFilterLayout::kBlocked512:
      return to_word64(BitCount{512});

    default:
      break;
  }
  BATT_PANIC() << "Invalid layout: " << (i32)layout;
  BATT_UNREACHABLE();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize block_count(BloomFilterLayout layout, Word64Count filter_size) noexcept
{
  switch (layout) {
    case BloomFilterLayout::kFlat:
      return 1;

    case BloomFilterLayout::kBlocked64:
      return to_bits(filter_size) / 64;

    case BloomFilterLayout::kBlocked512:
      return to_bits(filter_size) / 512;

    default:
      break;
  }
  BATT_PANIC() << "Invalid layout: " << (i32)layout;
  BATT_UNREACHABLE();
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
namespace {

const double ln2 = std::log(2);

RealBitCount bits_per_key_from_hash_count(HashCount hash_count) noexcept
{
  return RealBitCount{(double)hash_count.value() / ln2};
}

RealBitCount bits_per_key_from_error_rate(FalsePositiveRate false_positive_rate) noexcept
{
  return RealBitCount{std::log2(false_positive_rate) / ln2};
}

FalsePositiveRate error_rate_from_bits_per_key(RealBitCount bits_per_key) noexcept
{
  return FalsePositiveRate{(double)1 / std::pow((double)2, bits_per_key * ln2)};
}

Word64Count pad_filter_size_for_layout(BloomFilterLayout layout, Word64Count nominal_size) noexcept
{
  switch (layout) {
    case BloomFilterLayout::kFlat:
      return nominal_size;

    case BloomFilterLayout::kBlocked64:
      return nominal_size;

    case BloomFilterLayout::kBlocked512:
      return to_word64(BitCount{batt::round_up_bits(9, to_bits(nominal_size).value())});

    default:
      break;
  }
  BATT_PANIC() << "Invalid layout: " << (i32)layout;
  BATT_UNREACHABLE();
}

Word64Count truncate_filter_size_for_layout(BloomFilterLayout layout,
                                            Word64Count nominal_size) noexcept
{
  switch (layout) {
    case BloomFilterLayout::kFlat:
      return nominal_size;

    case BloomFilterLayout::kBlocked64:
      return nominal_size;

    case BloomFilterLayout::kBlocked512:
      return to_word64(BitCount{batt::round_down_bits(9, to_bits(nominal_size).value())});

    default:
      break;
  }
  BATT_PANIC() << "Invalid layout: " << (i32)layout;
  BATT_UNREACHABLE();
}

Word64Count filter_size_from(BloomFilterLayout layout, ItemCount key_count,
                             RealBitCount bits_per_key) noexcept
{
  const Word64Count nominal_size =
      to_word64(BitCount{(usize)std::ceil(bits_per_key * (double)key_count.value())});

  return pad_filter_size_for_layout(layout, nominal_size);
}

ItemCount key_count_from(Word64Count filter_size, RealBitCount bits_per_key) noexcept
{
  return ItemCount{(usize)std::floor((double)to_bits(filter_size).value() / bits_per_key)};
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ BloomFilterConfig BloomFilterConfig::from(BloomFilterLayout layout,  //
                                                     ItemCount key_count,       //
                                                     FalsePositiveRate false_positive_rate) noexcept
{
  const RealBitCount bits_per_key = bits_per_key_from_error_rate(false_positive_rate);

  return BloomFilterConfig{
      .layout = layout,
      .filter_size = filter_size_from(layout, key_count, bits_per_key),
      .key_count = key_count,
      .hash_count = hash_count_from_bits_per_key(bits_per_key),
      .bits_per_key = bits_per_key,
      .false_positive_rate = false_positive_rate,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ BloomFilterConfig BloomFilterConfig::from(BloomFilterLayout layout,  //
                                                     ItemCount key_count,       //
                                                     RealBitCount nominal_bits_per_key) noexcept
{
  const Word64Count filter_size = filter_size_from(layout, key_count, nominal_bits_per_key);
  const RealBitCount filter_bits{(double)to_bits(filter_size).value()};
  const RealBitCount bits_per_key{filter_bits.value() / (double)key_count};

  BATT_CHECK_GE(bits_per_key, nominal_bits_per_key);

  return BloomFilterConfig{
      .layout = layout,
      .filter_size = filter_size,
      .key_count = key_count,
      .hash_count = hash_count_from_bits_per_key(bits_per_key),
      .bits_per_key = bits_per_key,
      .false_positive_rate = error_rate_from_bits_per_key(bits_per_key),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ BloomFilterConfig BloomFilterConfig::from(BloomFilterLayout layout,         //
                                                     Word64Count nominal_filter_size,  //
                                                     RealBitCount bits_per_key) noexcept
{
  const Word64Count filter_size = truncate_filter_size_for_layout(layout, nominal_filter_size);

  return BloomFilterConfig{
      .layout = layout,
      .filter_size = filter_size,
      .key_count = key_count_from(filter_size, bits_per_key),
      .hash_count = hash_count_from_bits_per_key(bits_per_key),
      .bits_per_key = bits_per_key,
      .false_positive_rate = error_rate_from_bits_per_key(bits_per_key),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ BloomFilterConfig BloomFilterConfig::from(BloomFilterLayout layout,         //
                                                     Word64Count nominal_filter_size,  //
                                                     HashCount hash_count) noexcept
{
  const Word64Count filter_size = truncate_filter_size_for_layout(layout, nominal_filter_size);
  const RealBitCount bits_per_key = bits_per_key_from_hash_count(hash_count);

  return BloomFilterConfig{
      .layout = layout,
      .filter_size = filter_size,
      .key_count = key_count_from(filter_size, bits_per_key),
      .hash_count = hash_count,
      .bits_per_key = bits_per_key,
      .false_positive_rate = error_rate_from_bits_per_key(bits_per_key),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ BloomFilterConfig BloomFilterConfig::from(BloomFilterLayout layout,         //
                                                     Word64Count nominal_filter_size,  //
                                                     ItemCount key_count) noexcept
{
  const Word64Count filter_size = truncate_filter_size_for_layout(layout, nominal_filter_size);
  const RealBitCount bits_per_key{(double)to_bits(filter_size).value() / (double)key_count.value()};

  return BloomFilterConfig{
      .layout = layout,
      .filter_size = filter_size,
      .key_count = key_count,
      .hash_count = hash_count_from_bits_per_key(bits_per_key),
      .bits_per_key = bits_per_key,
      .false_positive_rate = error_rate_from_bits_per_key(bits_per_key),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Word64Count BloomFilterConfig::block_size() const noexcept
{
  return ::llfs::block_size(this->layout, this->filter_size);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize BloomFilterConfig::block_count() const noexcept
{
  return ::llfs::block_count(this->layout, this->filter_size);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize BloomFilterConfig::word_count() const noexcept
{
  return this->filter_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const BloomFilterConfig& t)
{
  return out << "BloomFilterConfig{"                                //
             << ".layout=" << t.layout                              //
             << ", .filter_size=" << t.filter_size                  //
             << ", .key_count=" << t.key_count                      //
             << ", .hash_count=" << t.hash_count                    //
             << ", .bits_per_key=" << t.bits_per_key                //
             << ", .false_positive_rate=" << t.false_positive_rate  //
             << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class PackedBloomFilter

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PackedBloomFilter::initialize(const BloomFilterConfig& config) noexcept
{
  [[maybe_unused]] static const bool init = [] {
    LLFS_LOG_INFO() <<
#ifdef __AVX512F__
        "AVX512 enabled"
#else
        "AVX512 NOT enabled"
#endif
        ;
    return true;
  }();

  BATT_CHECK_NE(config.word_count(), 0);
  BATT_CHECK_NE(config.block_count(), 0);

  this->word_count_ = config.word_count();
  this->block_count_ = config.block_count();
  this->hash_count_ = config.hash_count;
  this->layout_ = static_cast<u8>(config.layout);
  this->word_count_pre_mul_shift_ = batt::log2_ceil(this->word_count_) + 1;
  this->word_count_post_mul_shift_ = 64 - this->word_count_pre_mul_shift_;
  this->block_count_pre_mul_shift_ = batt::log2_ceil(this->block_count_) + 1;
  this->block_count_post_mul_shift_ = 64 - this->block_count_pre_mul_shift_;

  this->check_invariants();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PackedBloomFilter::check_invariants() const noexcept
{
  BATT_CHECK_LE(this->block_size() * this->block_count(), this->word_count());

  BATT_CHECK(this->layout() == BloomFilterLayout::kFlat ||
             this->layout() == BloomFilterLayout::kBlocked64 ||
             this->layout() == BloomFilterLayout::kBlocked512);

  BATT_CHECK_EQ(this->word_count_pre_mul_shift_ + this->word_count_post_mul_shift_, 64);
  BATT_CHECK_EQ(this->block_count_pre_mul_shift_ + this->block_count_post_mul_shift_, 64);

  BATT_CHECK_LE(this->word_count() * 2, u64{1} << this->word_count_pre_mul_shift_);
  BATT_CHECK_LE(this->block_count() * 2, u64{1} << this->block_count_pre_mul_shift_);

  switch (this->layout()) {
    case BloomFilterLayout::kFlat:
      break;

    case BloomFilterLayout::kBlocked64:
      break;

    case BloomFilterLayout::kBlocked512:
      BATT_CHECK_EQ(this->word_count() & 0b111, 0)
          << "The (64-bit) word count must be a multiple of 8 for 512-bit blocked layout";
      break;

    default:
      BATT_PANIC() << "Invalid layout=" << this->layout();
      BATT_UNREACHABLE();
      break;
  }
}

}  //namespace llfs
