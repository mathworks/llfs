//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_CONTEXT_HPP
#define LLFS_STORAGE_CONTEXT_HPP

#include <llfs/packed_config.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/storage_object_info.hpp>

#include <batteries/shared_ptr.hpp>

#include <string>

namespace llfs {

class StorageContext : public batt::RefCounted<StorageContext>
{
 public:
  StorageContext(const StorageContext&) = delete;
  StorageContext& operator=(const StorageContext&) = delete;

  virtual ~StorageContext() = default;

  virtual batt::SharedPtr<StorageObjectInfo> find_object_by_uuid(
      const boost::uuids::uuid& uuid) = 0;

 protected:
  StorageContext() = default;
};

}  // namespace llfs

#endif  // LLFS_STORAGE_CONTEXT_HPP
