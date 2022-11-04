//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_STATUS_OR_HPP
#define LLFS_PACKED_STATUS_OR_HPP

#include <llfs/data_packer.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/packed_status.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/status.hpp>

#include <type_traits>

namespace llfs {

template <typename T>
struct PackedStatusOr {
  // Set to 1 if this struct is followed by a `T`, 0 if it is followed by a PackedStatus.
  //
  little_u8 ok;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const T& value() const noexcept
  {
    BATT_CHECK_NE(this->ok, 0u);

    return *reinterpret_cast<const T*>(this + 1);
  }

  const PackedStatus& status() const noexcept
  {
    //----- --- -- -  -  -   -
    static const PackedStatus* packed_ok_status = [] {
      static std::array<char, 64> buffer;

      DataPacker packer{MutableBuffer{buffer.data(), buffer.size()}};
      const PackedStatus* packed = pack_object(batt::OkStatus(), &packer);
      BATT_CHECK_NOT_NULLPTR(packed);

      return packed;
    }();
    //----- --- -- -  -  -   -

    if (this->ok != 0u) {
      return *packed_ok_status;
    }

    return *reinterpret_cast<const PackedStatus*>(this + 1);
  }
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedStatusOr<int>), 1u);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
usize packed_sizeof(const PackedStatusOr<T>& status_or)
{
  if (status_or.ok == 0u) {
    return sizeof(PackedStatusOr<T>) + packed_sizeof(status_or.status());
  } else {
    return sizeof(PackedStatusOr<T>) + packed_sizeof(status_or.value());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T, typename PackedT = PackedTypeFor<T>>
usize packed_sizeof(const batt::StatusOr<T>& status_or)
{
  if (status_or.ok()) {
    return sizeof(PackedStatusOr<PackedT>) + packed_sizeof(*status_or);
  } else {
    return sizeof(PackedStatusOr<PackedT>) + packed_sizeof(status_or.status());
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
// TODO [tastolfi 2022-11-04] unpack_object

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T, typename PackedT>
PackedStatusOr<PackedT>* pack_object_to(const batt::StatusOr<T>& object,
                                        PackedStatusOr<PackedT>* packed_object, DataPacker* packer)
{
  if (object.ok()) {
    packed_object->ok = 1;

    if (pack_object(*object, packer) == nullptr) {
      return nullptr;
    }
  } else {
    packed_object->ok = 0;

    if (pack_object(object.status(), packer) == nullptr) {
      return nullptr;
    }
  }

  return packed_object;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
std::ostream& operator<<(std::ostream& out, const PackedStatusOr<T>& t)
{
  if (t.ok == 0u) {
    return out << "Status{" << t.status() << "}";
  }
  return out << "Ok{" << batt::make_printable(t.value()) << "}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
batt::Status validate_packed_value(const llfs::PackedStatusOr<T>& packed, const void* buffer_data,
                                   std::size_t buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));
  if (packed.ok == 0u) {
    return ::llfs_validate_packed_value_helper(packed.status(),
                                               ConstBuffer{buffer_data, buffer_size});
  } else {
    return ::llfs_validate_packed_value_helper(packed.value(),
                                               ConstBuffer{buffer_data, buffer_size});
  }
}

}  // namespace llfs

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

namespace batt {

template <typename T, typename PackedT = ::llfs::PackedTypeFor<T>>
inline [[maybe_unused]] ::batt::StaticType<::llfs::PackedStatusOr<PackedT>> llfs_packed_type_for(
    ::batt::StaticType<StatusOr<T>>)
{
  return {};
}

}  // namespace batt

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

static_assert(std::is_same_v<llfs::PackedTypeFor<batt::StatusOr<batt::Status>>,
                             llfs::PackedStatusOr<llfs::PackedStatus>>,
              "");

static_assert(std::is_same_v<llfs::PackedTypeFor<batt::StatusOr<llfs::little_u64>>,
                             llfs::PackedStatusOr<llfs::little_u64>>,
              "");

#endif  // LLFS_PACKED_STATUS_OR_HPP
