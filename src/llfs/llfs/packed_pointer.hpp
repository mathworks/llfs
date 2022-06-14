#pragma once
#ifndef LLFS_PACKED_POINTER_HPP
#define LLFS_PACKED_POINTER_HPP

#include <llfs/buffer.hpp>
#include <llfs/int_types.hpp>

#include <batteries/assert.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/stream_util.hpp>

#include <boost/operators.hpp>

#include <memory>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// PackedPointer<T> - a pointer to data that lives in the same data block (either slot or page). The
// pointer is always forward; this means we do not permit reference cycles.
//
template <typename T>
struct PackedPointer
    : boost::equality_comparable<PackedPointer<T>>
    , boost::totally_ordered<PackedPointer<T>> {
  using value_type = T;

  little_u32 offset;

  PackedPointer() = default;
  PackedPointer(const PackedPointer&) = delete;
  PackedPointer& operator=(const PackedPointer&) = delete;

  const void* get_raw_address() const
  {
    return this->get();
  }

  const T* get() const
  {
    return reinterpret_cast<const T*>(reinterpret_cast<const u8*>(this) + this->offset);
  }

  template <typename Dst>
  void reset(T* ptr, Dst* dst)
  {
    BATT_CHECK(dst->contains(this));
    BATT_CHECK(dst->contains(ptr)) << BATT_INSPECT((const void*)dst->buffer_begin())
                                   << BATT_INSPECT((const void*)dst->buffer_end())
                                   << BATT_INSPECT(sizeof(T)) << BATT_INSPECT((const void*)ptr);
    BATT_CHECK_LT((const void*)this, (const void*)ptr);

    this->offset = byte_distance(this, ptr);
  }

  const T* operator->() const
  {
    return this->get();
  }

  const T& operator*() const
  {
    return *this->get();
  }

  explicit operator bool() const
  {
    return this->offset != 0;
  }

  auto debug_dump(const void* base) const
  {
    return [base, this](std::ostream& out) {
      out << "[" << byte_distance(base, this) << ".." << byte_distance(base, this + 1)
          << "] PackedPointer<" << typeid(T).name() << "> {.offset=" << this->offset.value();
      if (this->offset != 0) {
        out << std::endl << this->get()->debug_dump(base) << std::endl;
      }
      out << "} PackedPointer";
    };
  }
};

template <typename U>
inline bool operator==(const PackedPointer<U>& l, const PackedPointer<U>& r)
{
  return l.get_raw_address() == r.get_raw_address();
}

template <typename U>
inline bool operator==(const std::nullptr_t&, const PackedPointer<U>& r)
{
  return 0 == r.offset;
}

template <typename U>
inline bool operator==(const PackedPointer<U>& l, const std::nullptr_t&)
{
  return l.offset == 0;
}

template <typename U>
inline bool operator<(const PackedPointer<U>& l, const PackedPointer<U>& r)
{
  return l.get_raw_address() == r.get_raw_address();
}

BATT_STATIC_ASSERT_EQ(sizeof(PackedPointer<int>), 4);

}  // namespace llfs

#endif  // LLFS_PACKED_POINTER_HPP
