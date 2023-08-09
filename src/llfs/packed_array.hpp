//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_ARRAY_HPP
#define LLFS_PACKED_ARRAY_HPP

#include <llfs/data_layout.hpp>
#include <llfs/optional.hpp>
#include <llfs/seq.hpp>

#include <batteries/optional.hpp>
#include <batteries/static_assert.hpp>

#include <cstring>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
struct PackedArray {
  enum Flags : u8 {
    kSizeInBytesSet = 0x01,
  };

  little_u24 item_count;
  Flags flags;
  little_u32 size_in_bytes;
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
    std::memset(&(this->item_count), 0, sizeof(PackedArray));
    this->item_count = count_arg;

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

  void initialize_size_in_bytes(usize size_in_bytes)
  {
    this->size_in_bytes = size_in_bytes;
    this->flags = static_cast<Flags>(this->flags | kSizeInBytesSet);
  }

  bool has_size_in_bytes() const
  {
    return (this->flags & kSizeInBytesSet);
  }

  Optional<usize> get_size_in_bytes() const
  {
    if (this->has_size_in_bytes()) {
      return this->size_in_bytes;
    }
    return 0;
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

template <typename T>
batt::Status validate_packed_value(const PackedArray<T>& a, const void* buffer_data,
                                   usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(a, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_byte_range(&a, packed_array_size(a.size(), batt::StaticType<T>{}),
                                             buffer_data, buffer_size));

  for (const auto& item : a) {
    BATT_REQUIRE_OK(validate_packed_value(item, buffer_data, buffer_size));
  }

  return batt::OkStatus();
}

template <boost::endian::order kOrder, typename T, usize kNBits>
batt::Status validate_packed_value(
    const PackedArray<boost::endian::endian_arithmetic<kOrder, T, kNBits>>& a,
    const void* buffer_data, usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(a, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_byte_range(&a, packed_array_size(a.size(), batt::StaticType<T>{}),
                                             buffer_data, buffer_size));

  return batt::OkStatus();
}

}  // namespace llfs

#endif  // LLFS_PACKED_ARRAY_HPP

#include <llfs/packed_seq.hpp>
