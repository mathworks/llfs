//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STORAGE_FILE_IPP
#define LLFS_STORAGE_FILE_IPP

#include <llfs/optional.hpp>

#include <boost/range/irange.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename PackedConfigT>
inline BoxedSeq<FileOffsetPtr<const PackedConfigT&>> StorageFile::find_objects_by_type(
    batt::StaticType<PackedConfigT>)
{
  return this->find_all_objects()  //
         | seq::filter_map([](const FileOffsetPtr<const PackedConfigSlot&> slot)
                               -> Optional<FileOffsetPtr<const PackedConfigT&>> {
             if (slot->tag == PackedConfigTagFor<PackedConfigT>::value) {
               return FileOffsetPtr<const PackedConfigT&>{
                   config_slot_cast<PackedConfigT>(slot.object), slot.file_offset};
             }
             return None;
           })  //
         | seq::boxed();
}

}  // namespace llfs

#endif  // LLFS_STORAGE_FILE_IPP
