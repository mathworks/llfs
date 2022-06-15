//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/volume_events.hpp>
//

#include <llfs/data_layout.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PrepareJob& obj)
{
  return sizeof(PackedPrepareJob) +                                                               //
         packed_array_size<PackedPageId>(batt::make_copy(obj.new_page_ids) | seq::count()) +      //
         packed_array_size<PackedPageId>(batt::make_copy(obj.deleted_page_ids) | seq::count()) +  //
         packed_array_size<PackedPageId>(trace_refs(obj.user_data) | seq::count()) +              //
         packed_sizeof(obj.user_data);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedPrepareJob* pack_object_to(const PrepareJob& obj, PackedPrepareJob* packed, DataPacker* dst)
{
  {
    PackedArray<PackedPageId>* packed_new_page_ids = pack_object(obj.new_page_ids, dst);
    if (!packed_new_page_ids) {
      return nullptr;
    }
    packed->new_page_ids.reset(packed_new_page_ids, dst);
  }
  //----- --- -- -  -  -   -
  {
    PackedArray<PackedPageId>* packed_deleted_page_ids = pack_object(obj.deleted_page_ids, dst);
    if (!packed_deleted_page_ids) {
      return nullptr;
    }
    packed->deleted_page_ids.reset(packed_deleted_page_ids, dst);
  }
  //----- --- -- -  -  -   -
  {
    PackedArray<PackedPageId>* packed_root_page_ids = pack_object(trace_refs(obj.user_data), dst);
    if (!packed_root_page_ids) {
      return nullptr;
    }
    packed->root_page_ids.reset(packed_root_page_ids, dst);
  }
  //----- --- -- -  -  -   -
  {
    PackedRawData* packed_user_data = pack_object(obj.user_data, dst);
    if (!packed_user_data) {
      return nullptr;
    }
    packed->user_data.reset(packed_user_data, dst);
  }
  //----- --- -- -  -  -   -

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<Ref<const PackedPrepareJob>> unpack_object(const PackedPrepareJob& packed, DataReader*)
{
  return as_cref(packed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedPrepareJob& obj)
{
  return sizeof(PackedPrepareJob) +              //
         packed_sizeof(*obj.new_page_ids) +      //
         packed_sizeof(*obj.deleted_page_ids) +  //
         packed_sizeof(*obj.root_page_ids) +     //
         packed_sizeof(*obj.user_data);
}

}  // namespace llfs
