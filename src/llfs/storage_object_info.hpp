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
#include <llfs/storage_file.hpp>

#include <batteries/shared_ptr.hpp>

#include <boost/multi_index/global_fun.hpp>

#include <string>

namespace llfs {

struct StorageObjectInfo : batt::RefCounted<StorageObjectInfo> {
  explicit StorageObjectInfo(batt::SharedPtr<StorageFile>&& storage_file,
                             FileOffsetPtr<const PackedConfigSlot&> p_config_slot) noexcept;

  batt::SharedPtr<StorageFile> storage_file;
  FileOffsetPtr<const PackedConfigSlot&> p_config_slot;
};

}  // namespace llfs

#endif  // LLFS_STORAGE_OBJECT_INFO_HPP
