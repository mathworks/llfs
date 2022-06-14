#pragma once
#ifndef LLFS_STORAGE_CONTEXT_HPP
#define LLFS_STORAGE_CONTEXT_HPP

#include <llfs/packed_config.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_size.hpp>
#include <llfs/storage_object_info.hpp>

#include <batteries/shared_ptr.hpp>

#include <boost/multi_index/global_fun.hpp>
#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index_container.hpp>
#include <boost/preprocessor/cat.hpp>
#include <boost/uuid/uuid.hpp>

#include <string>

namespace llfs {

class StorageContext : public batt::RefCounted<StorageContext>
{
 public:
#define LLFS_STORAGE_OBJECT_KEY_EXTRACTOR(TYPE, FIELD_NAME, EXTRACTOR_NAME)                        \
  static u32 BOOST_PP_CAT(get_, FIELD_NAME)(const batt::SharedPtr<StorageObjectInfo>& info)        \
  {                                                                                                \
    return info->FIELD_NAME;                                                                       \
  }                                                                                                \
  using EXTRACTOR_NAME = boost::multi_index::global_fun<const batt::SharedPtr<StorageObjectInfo>&, \
                                                        TYPE, &BOOST_PP_CAT(get_, FIELD_NAME)>

  LLFS_STORAGE_OBJECT_KEY_EXTRACTOR(u32, tag, StorageObjectKey);

  using ObjectIndex = boost::multi_index::multi_index_container<
      batt::SharedPtr<StorageObjectInfo>,
      boost::multi_index::indexed_by<boost::multi_index::hashed_non_unique<StorageObjectKey>>>;

  batt::Watch<page_device_id_int> next_available_device_id;
  batt::Mutex<ObjectIndex> index;
};

}  // namespace llfs

#endif  // LLFS_STORAGE_CONTEXT_HPP
