//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_context.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::SharedPtr<StorageObjectInfo> StorageContext::find_object_by_uuid(
    const boost::uuids::uuid& uuid) /*override*/
{
  auto iter = this->index_.find(uuid);
  if (iter == this->index_.end()) {
    return nullptr;
  }
  return iter->second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status StorageContext::add_file(const batt::SharedPtr<StorageFile>& file)
{
  file->find_all_objects()  //
      | seq::for_each([&](const FileOffsetPtr<const PackedConfigSlot&>& slot) {
          this->index_.emplace(slot->uuid,
                               batt::make_shared<StorageObjectInfo>(batt::make_copy(file), slot));
        });

  return OkStatus();
}

}  // namespace llfs
