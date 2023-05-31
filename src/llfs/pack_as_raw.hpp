//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACK_AS_RAW_HPP
#define LLFS_PACK_AS_RAW_HPP

#include <llfs/define_packed_type.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/ref.hpp>
#include <llfs/slot_parse.hpp>
#include <llfs/status.hpp>

#include <string_view>

namespace llfs {

class DataPacker;
class DataReader;

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Wrapper for byte data; forces data to be packed as raw bytes WITHOUT a PackedBytes header (like
// normal std::string/std::string_view packing).
//
struct PackAsRawData {
  std::string_view bytes;
};

// Returns a PackAsRawData object pointing to the data referenced by `bytes`.
//
PackAsRawData pack_as_raw(std::string_view bytes);

// Returns `pack_as_raw_data.size()` (since there is no header size to account for).
//
usize packed_sizeof(const PackAsRawData& pack_as_raw_data);

// Packs the specified data as raw bytes into dst.  If there isn't enough space available in the
// destination buffer, returns nullptr; otherwise returns a pointer to the start of the packed data.
//
void* pack_object(const PackAsRawData& pack_as_raw_data, DataPacker* dst);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// Raw (opaque) data must be at the end of a slot.
//
struct PackedRawData {
  PackedRawData(const PackedRawData&) = delete;
  PackedRawData& operator=(const PackedRawData&) = delete;

  little_u8 bytes[1];
};

static_assert(sizeof(PackedRawData) == 1);

LLFS_DEFINE_PACKED_TYPE_FOR(PackAsRawData, PackedRawData);

usize packed_sizeof(const PackedRawData&);

StatusOr<Ref<const PackedRawData>> unpack_object(const PackedRawData& packed, DataReader*);

std::string_view raw_data_from_slot(const SlotParse& slot, const PackedRawData* packed);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

}  // namespace llfs

#endif  // LLFS_PACK_AS_RAW_HPP
