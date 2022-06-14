#pragma once
#ifndef LLFS_PACKED_ARRAY_HPP
#define LLFS_PACKED_ARRAY_HPP

#include <llfs/data_layout.hpp>
#include <llfs/seq.hpp>

#include <batteries/static_assert.hpp>

#include <cstring>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
struct PackedArray {
  little_u24 item_count;
  little_u8 reserved_[5];
  T items[0];

  // This struct must never be copied since that would invalidate `items`.
  //
  PackedArray(const PackedArray&) = delete;
  PackedArray& operator=(const PackedArray&) = delete;

  using value_type = T;
  using iterator = T*;
  using const_iterator = const T*;

  template <typename I>
  void initialize(I count_arg)
  {
    this->item_count = count_arg;
    std::memset(this->reserved_, 0, sizeof(this->reserved_));

    BATT_CHECK_EQ(count_arg, this->item_count.value());
  }

  iterator begin()
  {
    return &this->items[0];
  }
  iterator end()
  {
    return &this->items[this->item_count.value()];
  }
  const_iterator begin() const
  {
    return &this->items[0];
  }
  const_iterator end() const
  {
    return &this->items[this->item_count.value()];
  }

  bool empty() const
  {
    return this->item_count == 0;
  }

  const T& front() const
  {
    return this->items[0];
  }

  const T& back() const
  {
    return this->items[this->item_count - 1];
  }

  T* data()
  {
    return this->items;
  }

  const T* data() const
  {
    return this->items;
  }

  usize size() const
  {
    return this->item_count;
  }

  T& operator[](usize index)
  {
    return this->items[index];
  }

  const T& operator[](usize index) const
  {
    return this->items[index];
  }

  auto debug_dump(const void* base) const
  {
    return [base, this](std::ostream& out) {
      out << "[" << byte_distance(base, this) << ".." << byte_distance(base, this + 1)
          << "] PackedArray<" << typeid(T).name() << "> {" << std::endl
          << " .size=" << this->item_count.value() << ";" << std::endl;

      for (const auto& item : *this) {
        out << " .item=" << item.debug_dump(base) << ";" << std::endl;
      }

      out << "} PackedArray;" << std::endl;
    };
  }
};

template <typename T>
inline auto as_seq(const PackedArray<T>& packed)
{
  return as_seq(packed.data(), packed.data() + packed.size());
}

BATT_STATIC_ASSERT_EQ(sizeof(PackedArray<char>), 8);
BATT_STATIC_ASSERT_EQ(sizeof(PackedArray<u64>), 8);

template <typename T>
inline usize packed_array_size(usize item_count, batt::StaticType<T> = {})
{
  const little_u24 packed_count = item_count;

  BATT_ASSERT_EQ(static_cast<usize>(packed_count.value()), item_count);

  return sizeof(PackedArray<T>) + sizeof(T) * item_count;
}

template <typename T>
inline usize packed_sizeof(const PackedArray<T>& a)
{
  return packed_array_size<T>(a.item_count);
}

}  // namespace llfs

#endif  // LLFS_PACKED_ARRAY_HPP

#include <llfs/packed_seq.hpp>
