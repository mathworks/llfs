//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_OBJECT_INFO_HPP
#define LLFS_STORAGE_OBJECT_INFO_HPP

#include <llfs/int_types.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>

#include <batteries/shared_ptr.hpp>

#include <boost/multi_index/global_fun.hpp>

#include <string>

namespace llfs {

struct StorageObjectInfo : batt::RefCounted<StorageObjectInfo> {
  // Must be one of the values defined in PackedConfigSlotBase::Tag.
  //
  u32 tag;

  // The unique identifier for this object.
  //
  boost::uuids::uuid uuid;

  // A human-friendly name for this object; doesn't need to be unique.
  //
  std::string common_name;

  // The file containing this object.
  //
  std::string file_path;

  // The offset within `file_path` where this object's config slot resides.
  //
  i64 config_file_offset;

  // If the storage object is associated with a PageDevice, the device_id within its
  // StorageContext/PageCache.
  //
  page_device_id_int device_id;

  // If the storage object is associated with a PageDevice, the page size in bytes of that device
  // (for sanity cross-checking).
  //
  PageSize page_size;

  // If the storage object is associated with a PageDevice, the number of pages in that device
  // (for sanity cross-checking).
  //
  PageCount page_count;
};

}  // namespace llfs

#endif  // LLFS_STORAGE_OBJECT_INFO_HPP
