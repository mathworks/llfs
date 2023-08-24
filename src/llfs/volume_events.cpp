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

#include <boost/uuid/uuid_io.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const PackedVolumeIds& t)
{
  return out << "PackedVolumeIds{.main_uuid=" << t.main_uuid
             << ", .recycler_uuid=" << t.recycler_uuid << ", .trimmer_uuid=" << t.trimmer_uuid
             << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool operator==(const VolumeAttachmentId& l, const VolumeAttachmentId& r)
{
  return l.client == r.client     //
         && l.device == r.device  //
      ;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const VolumeAttachmentId& id)
{
  return out << "{.client=" << id.client << ", .device=" << id.device.value() << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PrepareJob& obj)
{
  return sizeof(PackedPrepareJob) +                                                               //
         packed_sizeof(obj.user_data) +                                                           //
         packed_array_size<PackedPageId>(trace_refs(obj.user_data) | seq::count()) +              //
         packed_array_size<PackedPageId>(batt::make_copy(obj.new_page_ids) | seq::count()) +      //
         packed_array_size<PackedPageId>(batt::make_copy(obj.deleted_page_ids) | seq::count()) +  //
         packed_array_size<little_page_device_id_int>(batt::make_copy(obj.page_device_ids) |
                                                      seq::count());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedPrepareJob* pack_object_to(const PrepareJob& obj, PackedPrepareJob* packed, DataPacker* dst)
{
  const usize user_data_size = packed_sizeof(obj.user_data);
  {
    Optional<MutableBuffer> user_data_buffer = dst->reserve_front(user_data_size);
    if (!user_data_buffer) {
      return nullptr;
    }
    DataPacker user_data_packer{*user_data_buffer};
    PackedRawData* packed_user_data = pack_object(obj.user_data, &user_data_packer);
    if (!packed_user_data) {
      return nullptr;
    }
    BATT_CHECK_EQ((const void*)packed_user_data, (const void*)(packed + 1));
  }

  // IMPORTANT: root_page_ids must be first (after user data)!
  {
    PackedArray<PackedPageId>* packed_root_page_ids = pack_object(trace_refs(obj.user_data), dst);
    if (!packed_root_page_ids) {
      return nullptr;
    }
    packed->root_page_ids.reset(packed_root_page_ids, dst);
  }
  //----- --- -- -  -  -   -
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
    PackedArray<little_page_device_id_int>* packed_page_device_ids =
        pack_object(obj.page_device_ids, dst);
    if (!packed_page_device_ids) {
      return nullptr;
    }
    packed->page_device_ids.reset(packed_page_device_ids, dst);
  }
  //----- --- -- -  -  -   -

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// TODO [tastolfi 2023-03-15] DEPRECATE (replace with unpack_cast/validate)
//
StatusOr<Ref<const PackedPrepareJob>> unpack_object(const PackedPrepareJob& packed, DataReader*)
{
  return as_cref(packed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status validate_packed_value(const PackedPrepareJob& packed, const void* buffer_data,
                             usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(*packed.root_page_ids, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(*packed.new_page_ids, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(*packed.deleted_page_ids, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(*packed.page_device_ids, buffer_data, buffer_size));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedPrepareJob& obj)
{
  return sizeof(PackedPrepareJob) +              //
         packed_sizeof(*obj.new_page_ids) +      //
         packed_sizeof(*obj.deleted_page_ids) +  //
         packed_sizeof(*obj.root_page_ids) +     //
         packed_sizeof(*obj.page_device_ids) +   //
         obj.user_data().size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_commit(const PrepareJob& obj)
{
  return sizeof(PackedCommitJob) + packed_sizeof(obj.user_data) +
         packed_array_size<PackedPageId>(trace_refs(obj.user_data) | seq::count());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_commit_slot(const PrepareJob& obj)
{
  return packed_sizeof_slot_with_payload_size(packed_sizeof_commit(obj));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const CommitJob& obj)
{
  BATT_CHECK_NOT_NULLPTR(obj.prepare_job);
  BATT_CHECK_NOT_NULLPTR(obj.prepare_job->root_page_ids.get());

  return sizeof(PackedCommitJob) + packed_sizeof(*obj.prepare_job->root_page_ids) +
         obj.prepare_job->user_data().size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedCommitJob& obj)
{
  return sizeof(PackedCommitJob) + packed_sizeof(*obj.root_page_ids) + obj.user_data().size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedCommitJob* pack_object_to(const CommitJob& obj, PackedCommitJob* packed, DataPacker* dst)
{
  packed->prepare_slot = obj.prepare_slot;
  packed->prepare_slot_size = packed_sizeof_slot(*obj.prepare_job);

  // Byte-wise copy the user data from the prepare job directly after the PackedCommitJob struct.
  //
  std::string_view user_data = obj.prepare_job->user_data();
  Optional<std::string_view> packed_user_data =
      dst->pack_raw_data(user_data.data(), user_data.size());
  if (!packed_user_data) {
    return nullptr;
  }
  BATT_CHECK_EQ((const void*)(packed + 1), (const void*)packed_user_data->data())
      << "User data must come right after the PackedCommitJob!";

  // Place the root_page_ids PackedArray after the user data; this allows us to use the offset
  // stored in the pointer (PackedCommitJob::root_page_ids) to derive the size of user data.
  //
  PackedArray<PackedPageId>* packed_root_page_ids = dst->pack_record<PackedArray<PackedPageId>>();
  if (!packed_root_page_ids) {
    return nullptr;
  }

  // Initialize the array and copy any page ids.
  //
  const usize id_count = obj.prepare_job->root_page_ids->size();
  if (id_count != 0) {
    if (!dst->pack_raw_data(obj.prepare_job->root_page_ids->data(),
                            sizeof(PackedPageId) * id_count)) {
      return nullptr;
    }
  }
  packed_root_page_ids->initialize(id_count);
  packed->root_page_ids.reset(packed_root_page_ids, dst);

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// TODO [tastolfi 2023-08-22] DEPRECATE (replace with unpack_cast/validate)
//
StatusOr<Ref<const PackedCommitJob>> unpack_object(const PackedCommitJob& packed, DataReader*)
{
  return as_cref(packed);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status validate_packed_value(const PackedCommitJob& packed, const void* buffer_data,
                             usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(*packed.root_page_ids, buffer_data, buffer_size));

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const VolumeTrimEvent&)
{
  return sizeof(PackedVolumeTrimEvent);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedVolumeTrimEvent&)
{
  return sizeof(PackedVolumeTrimEvent);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedVolumeTrimEvent* pack_object_to(const VolumeTrimEvent& object, PackedVolumeTrimEvent* packed,
                                      DataPacker*)
{
  packed->old_trim_pos = object.old_trim_pos;
  packed->new_trim_pos = object.new_trim_pos;
  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<VolumeTrimEvent> unpack_object(const PackedVolumeTrimEvent& packed, DataReader*)
{
  return VolumeTrimEvent{
      .old_trim_pos = packed.old_trim_pos,
      .new_trim_pos = packed.new_trim_pos,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status validate_packed_value(const PackedVolumeTrimEvent& packed, const void* buffer_data,
                             usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::ostream& operator<<(std::ostream& out, const VolumeTrimEvent& t)
{
  return out << "VolumeTrimEvent"                     //
             << "{.old_trim_pos=" << t.old_trim_pos   //
             << ", .new_trim_pos=" << t.new_trim_pos  //
             << ",}";
}

}  // namespace llfs
