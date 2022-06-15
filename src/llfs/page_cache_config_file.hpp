//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_CACHE_CONFIG_FILE_HPP
#define LLFS_PAGE_CACHE_CONFIG_FILE_HPP

#include <llfs/data_layout.hpp>
#include <llfs/define_packed_type.hpp>
#include <llfs/file_segment_ref.hpp>
#include <llfs/packed_pointer.hpp>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <string>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageArenaFileRef {
  FileSegmentRef arena_file;
  u32 page_device_id;
  boost::uuids::uuid page_device_uuid;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedPageArenaFileRef {
  PackedFileSegmentRef arena_file;
  little_u32 page_device_id;
  u8 reserved_[4];
  boost::uuids::uuid page_device_uuid;
};

LLFS_DEFINE_PACKED_TYPE_FOR(PageArenaFileRef, PackedPageArenaFileRef);

usize packed_sizeof(const PageArenaFileRef& obj);

PackedPageArenaFileRef* pack_object_to(const PageArenaFileRef& obj, PackedPageArenaFileRef* packed,
                                       DataPacker* dst);

StatusOr<PageArenaFileRef> unpack_object(const PackedPageArenaFileRef& packed, DataReader* src);

inline std::ostream& operator<<(std::ostream& out, const PageArenaFileRef& t)
{
  return out << "PageArenaFileRef{.arena_file=" << t.arena_file
             << ", .page_device_id=" << t.page_device_id
             << ", page_device_uuid=" << t.page_device_uuid << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PageCacheConfigFile {
  std::vector<PageArenaFileRef> arenas;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

struct PackedPageCacheConfigFile {
  static constexpr u64 kMagic = 0x6783fc62eb422d0eull;

  big_u64 magic;
  u8 reserved_[48];
  PackedPointer<PackedArray<PackedPageArenaFileRef>> arenas;
};

LLFS_DEFINE_PACKED_TYPE_FOR(PageCacheConfigFile, PackedPageCacheConfigFile);

usize packed_sizeof(const PageCacheConfigFile& obj);

PackedPageCacheConfigFile* pack_object_to(const PageCacheConfigFile& obj,
                                          PackedPageCacheConfigFile* packed, DataPacker* dst);

StatusOr<PageCacheConfigFile> unpack_object(const PackedPageCacheConfigFile& packed,
                                            DataReader* src);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

StatusOr<PageCacheConfigFile> read_page_cache_config_file(std::string_view filename);

Status write_page_cache_config_file(std::string_view filename,
                                    const PageCacheConfigFile& cache_config);

}  // namespace llfs

#endif  // LLFS_PAGE_CACHE_CONFIG_FILE_HPP
