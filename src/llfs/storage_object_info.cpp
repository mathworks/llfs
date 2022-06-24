//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_object_info.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ StorageObjectInfo::StorageObjectInfo(
    batt::SharedPtr<StorageFile>&& storage_file,
    FileOffsetPtr<const PackedConfigSlot&> p_config_slot) noexcept
    : storage_file{std::move(storage_file)}
    , p_config_slot{std::move(p_config_slot)}
{
}

}  // namespace llfs
