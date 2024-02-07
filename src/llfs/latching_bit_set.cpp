//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/latching_bit_set.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ LatchingBitSet::LatchingBitSet(usize n) noexcept : upper_bound_{n}
{
  usize total_size = 0;
  usize level_size = this->upper_bound_;

  this->start_of_level_.emplace_back(0);

  batt::SmallVec<u64, 12> last_word;

  for (;;) {
    const usize leftover_bits = level_size % 64;
    if (leftover_bits == 0) {
      last_word.emplace_back(0);
    } else {
      last_word.emplace_back(~((u64{1} << leftover_bits) - 1));
    }

    level_size = (level_size + 63) / 64;
    total_size += level_size;
    if (level_size <= 1) {
      break;
    }
    this->start_of_level_.emplace_back(this->start_of_level_.back() + level_size);
  }

  if (total_size > 0) {
    this->data_.reset(new u64[total_size]);
    std::memset(this->data_.get(), 0, sizeof(u64) * total_size);

    // Set any leftover bits at the end of the last word in each level to 1, to simplify the code
    // to check for a block containing all 1's.
    //
    for (usize i = 0; i < this->start_of_level_.size() - 1; ++i) {
      const usize last_word_offset = this->start_of_level_[i + 1] - 1;
      this->data_.get()[last_word_offset] = last_word[i];
    }
    this->data_.get()[this->start_of_level_.back()] = last_word.back();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize LatchingBitSet::upper_bound() const noexcept
{
  return this->upper_bound_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize LatchingBitSet::first_missing() const noexcept
{
  if (this->is_full()) {
    return this->upper_bound();
  }

  usize depth = this->start_of_level_.size();
  usize lower_bound = 0;
  while (depth > 0) {
    --depth;

    const usize index = this->start_of_level_[depth] + lower_bound;

    // Even if the set wasn't full
    //
    if (depth + 1 < this->start_of_level_.size() && index >= this->start_of_level_[depth + 1]) {
      return this->upper_bound();
    }

    const u64 value = this->data()[index].load();
    i32 first_zero_bit = [&] {
      if (value == 0) {
        return 0;
      }
      return __builtin_ctzll(~value);
    }();

    lower_bound = lower_bound * 64 + first_zero_bit;
  }

  return lower_bound;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool LatchingBitSet::contains(usize i) noexcept
{
  BATT_CHECK_LT(i, this->upper_bound());

  const usize word_i = i / 64;
  const usize bit_i = i % 64;
  const u64 mask = u64{1} << bit_i;

  return (this->data()[word_i].load() & mask) != 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool LatchingBitSet::insert(usize i) noexcept
{
  BATT_CHECK_LT(i, this->upper_bound());

  bool changed = false;

  usize depth = 0;

  for (;;) {
    const usize word_i = i / 64;
    const usize bit_i = i % 64;
    const u64 mask = u64{1} << bit_i;

    const u64 old_value = this->data()[word_i + this->start_of_level_[depth]].fetch_or(mask);
    const u64 new_value = old_value | mask;

    changed = changed || (old_value != new_value);

    if (!changed || batt::bit_count(new_value) < 64) {
      break;
    }

    ++depth;
    if (depth == this->start_of_level_.size()) {
      break;
    }
    i = word_i;
  }

  return changed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool LatchingBitSet::is_full() const noexcept
{
  if (this->upper_bound() == 0u) {
    return true;
  }
  return this->data()[this->start_of_level_.back()].load() == ~u64{0};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::atomic<u64>* LatchingBitSet::data() const noexcept
{
  static_assert(sizeof(std::atomic<u64>) == sizeof(u64));

  return reinterpret_cast<std::atomic<u64>*>(this->data_.get());
}

}  //namespace llfs
