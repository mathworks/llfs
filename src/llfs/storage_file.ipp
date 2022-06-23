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
  return as_seq(this->config_blocks_)  //
         | seq::map([](const std::unique_ptr<StorageFileConfigBlock>& p_config_block) {
             return as_seq(boost::irange<usize>(0, p_config_block->get_const().slots.size()))  //
                    | seq::filter_map([p_config_block = p_config_block.get()](usize slot_index)
                                          -> Optional<FileOffsetPtr<const PackedConfigT&>> {
                        const FileOffsetPtr<const PackedConfigSlot&> slot =
                            p_config_block->get_const_ptr().get_slot(slot_index);
                        if (slot->tag == PackedConfigTagFor<PackedConfigT>::value) {
                          return FileOffsetPtr<const PackedConfigT&>{
                              config_slot_cast<PackedConfigT>(slot.object), slot.file_offset};
                        }
                        return None;
                      });
           })              //
         | seq::flatten()  //
         | seq::boxed();
  ;
}

}  // namespace llfs

#endif  // LLFS_STORAGE_FILE_IPP
