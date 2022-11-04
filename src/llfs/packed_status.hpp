//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKED_STATUS_HPP
#define LLFS_PACKED_STATUS_HPP

#include <llfs/data_packer.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_bytes.hpp>
#include <llfs/unpack_cast.hpp>

#include <batteries/static_assert.hpp>
#include <batteries/status.hpp>

#include <type_traits>

namespace llfs {

struct PackedStatus {
  little_u32 code;
  PackedBytes group;
  PackedBytes message;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedStatus), 20);

usize packed_sizeof(const PackedStatus& packed_status);

usize packed_sizeof(const batt::Status& status);

StatusOr<batt::Status> unpack_object(const PackedStatus& packed_status, DataReader*);

PackedStatus* pack_object_to(const batt::Status& status, PackedStatus* packed_status,
                             DataPacker* packer);

std::ostream& operator<<(std::ostream& out, const PackedStatus& t);

batt::Status validate_packed_value(const llfs::PackedStatus&, const void*, std::size_t);

}  // namespace llfs

namespace batt {

LLFS_DEFINE_PACKED_TYPE_FOR(Status, ::llfs::PackedStatus);

}  // namespace batt

static_assert(std::is_same_v<llfs::PackedTypeFor<batt::Status>, llfs::PackedStatus>, "");
static_assert(std::is_same_v<llfs::UnpackedTypeFor<llfs::PackedStatus>, batt::Status>, "");

#endif  // LLFS_PACKED_STATUS_HPP
