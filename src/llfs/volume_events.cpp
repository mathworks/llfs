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
  const usize prepare_job_size = sizeof(PackedPrepareJob);

  const usize new_ids_size =
      packed_array_size<PackedPageId>(batt::make_copy(obj.new_page_ids) | seq::count());

  const usize deleted_ids_size =
      packed_array_size<PackedPageId>(batt::make_copy(obj.deleted_page_ids) | seq::count());

  const usize root_ids_size =
      packed_array_size<PackedPageId>(trace_refs(obj.user_data)  //
                                      | seq::filter([](const PageId& page_id) {
                                          return page_id.is_valid();
                                        })  //
                                      | seq::count());

  const usize device_ids_size = packed_array_size<little_page_device_id_int>(
      batt::make_copy(obj.page_device_ids) | seq::count());

  const usize user_data_size = packed_sizeof(obj.user_data);

  LLFS_DVLOG(1) << BATT_INSPECT(prepare_job_size) << BATT_INSPECT(new_ids_size)
                << BATT_INSPECT(deleted_ids_size) << BATT_INSPECT(root_ids_size)
                << BATT_INSPECT(device_ids_size) << BATT_INSPECT(user_data_size)
                << BATT_INSPECT(obj.user_data.name_of_type());

  return prepare_job_size +  //
         new_ids_size +      //
         deleted_ids_size +  //
         root_ids_size +     //
         device_ids_size +   //
         user_data_size;
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
    PackedArray<PackedPageId>* packed_root_page_ids =
        pack_object(pack_seq_as_array(trace_refs(obj.user_data)  //
                                      | seq::filter([](const PageId& page_id) {
                                          return page_id.is_valid();
                                        })),
                    dst);
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
  const usize prepare_job_size = sizeof(PackedPrepareJob);
  const usize new_ids_size = packed_sizeof(*obj.new_page_ids);
  const usize deleted_ids_size = packed_sizeof(*obj.deleted_page_ids);
  const usize root_ids_size = packed_sizeof(*obj.root_page_ids);
  const usize device_ids_size = packed_sizeof(*obj.page_device_ids);
  const usize user_data_size = obj.user_data().size();

  LLFS_DVLOG(1) << BATT_INSPECT(prepare_job_size) << BATT_INSPECT(new_ids_size)
                << BATT_INSPECT(deleted_ids_size) << BATT_INSPECT(root_ids_size)
                << BATT_INSPECT(device_ids_size) << BATT_INSPECT(user_data_size);

  return prepare_job_size +  //
         new_ids_size +      //
         deleted_ids_size +  //
         root_ids_size +     //
         device_ids_size +   //
         user_data_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof_commit(const PrepareJob& obj)
{
  return sizeof(PackedCommitJob) + packed_sizeof(obj.user_data) +
         packed_array_size<PackedPageId>(trace_refs(obj.user_data) |
                                         seq::filter([](const PageId& page_id) {
                                           return page_id.is_valid();
                                         })  //
                                         | seq::count());
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
  BATT_CHECK_NOT_NULLPTR(obj.packed_prepare);
  BATT_CHECK_NOT_NULLPTR(obj.packed_prepare->root_page_ids.get());

  const usize commit_job_size = sizeof(PackedCommitJob);
  const usize root_ids_size = packed_sizeof(*obj.packed_prepare->root_page_ids);
  const usize user_data_size = obj.packed_prepare->user_data().size();

  LLFS_DVLOG(1) << BATT_INSPECT(commit_job_size) << BATT_INSPECT(root_ids_size)
                << BATT_INSPECT(user_data_size);

  return commit_job_size + root_ids_size + user_data_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedCommitJob& obj)
{
  const usize commit_job_size = sizeof(PackedCommitJob);
  const usize root_ids_size = packed_sizeof(*obj.root_page_ids);
  const usize user_data_size = obj.user_data().size();

  LLFS_DVLOG(1) << BATT_INSPECT(commit_job_size) << BATT_INSPECT(root_ids_size)
                << BATT_INSPECT(user_data_size);

  return commit_job_size + root_ids_size + user_data_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedCommitJob* pack_object_to(const CommitJob& obj, PackedCommitJob* packed, DataPacker* dst)
{
  packed->prepare_slot_offset = obj.prepare_slot_offset;
  packed->prepare_slot_size = packed_sizeof_slot(*obj.packed_prepare);

  // Byte-wise copy the user data from the prepare job directly after the PackedCommitJob struct.
  //
  std::string_view user_data = obj.packed_prepare->user_data();
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
  const usize id_count = obj.packed_prepare->root_page_ids->size();
  if (id_count != 0) {
    if (!dst->pack_raw_data(obj.packed_prepare->root_page_ids->data(),
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
