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
std::ostream& operator<<(std::ostream& out, const VolumeAttachmentId& id)
{
  return out << "{.client=" << id.client << ", .device=" << id.device.value() << ",}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PrepareJob& obj)
{
  return sizeof(PackedPrepareJob) +                                                               //
         packed_array_size<PackedPageId>(batt::make_copy(obj.new_page_ids) | seq::count()) +      //
         packed_array_size<PackedPageId>(batt::make_copy(obj.deleted_page_ids) | seq::count()) +  //
         packed_array_size<little_page_device_id_int>(batt::make_copy(obj.page_device_ids) |
                                                      seq::count()) +                 //
         packed_array_size<PackedPageId>(trace_refs(obj.user_data) | seq::count()) +  //
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
    PackedArray<little_page_device_id_int>* packed_page_device_ids =
        pack_object(obj.page_device_ids, dst);
    if (!packed_page_device_ids) {
      return nullptr;
    }
    packed->page_device_ids.reset(packed_page_device_ids, dst);
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
// TODO [tastolfi 2023-03-15] DEPRECATE (replace with unpack_cast/validate)
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
         packed_sizeof(*obj.page_device_ids) +   //
         packed_sizeof(*obj.user_data);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const TrimmedPrepareJob& object)
{
  return sizeof(PackedTrimmedPrepareJob) +
         sizeof(PackedPageId) * (batt::make_copy(object.page_ids) | batt::seq::count());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedTrimmedPrepareJob& packed)
{
  return sizeof(PackedTrimmedPrepareJob) + sizeof(PackedPageId) * packed.page_ids.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedTrimmedPrepareJob* pack_object_to(const TrimmedPrepareJob& object,
                                        PackedTrimmedPrepareJob* packed, DataPacker* dst)
{
  packed->prepare_slot = object.prepare_slot;
  packed->page_ids.initialize(0u);

  bool error = false;
  usize count = 0;
  batt::make_copy(object.page_ids)  //
      | batt::seq::for_each([&count, &error, dst](PageId page_id) {
          if (pack_object(page_id, dst) == nullptr) {
            error = true;
            return batt::seq::LoopControl::kBreak;
          }
          ++count;
          return batt::seq::LoopControl::kContinue;
        });

  if (error) {
    return nullptr;
  }

  packed->page_ids.item_count = count;

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<TrimmedPrepareJob> unpack_object(const PackedTrimmedPrepareJob& packed,
                                          DataReader* /*src*/)
{
  TrimmedPrepareJob object;

  object.prepare_slot = packed.prepare_slot;
  object.page_ids = as_seq(packed.page_ids)  //
                    | batt::seq::map([](const PackedPageId& page_id) {
                        return page_id.unpack();
                      })  //
                    | batt::seq::boxed();

  return object;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status validate_packed_value(const PackedTrimmedPrepareJob& packed, const void* buffer_data,
                             usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));
  BATT_REQUIRE_OK(
      validate_packed_byte_range(&packed, packed_sizeof(packed), buffer_data, buffer_size));

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const VolumeTrimEvent& object)
{
  const usize n_committed = [&] {
    if (object.committed_jobs) {
      return batt::make_copy(object.committed_jobs) | batt::seq::count();
    } else {
      return usize{0};
    }
  }();

  return sizeof(PackedVolumeTrimEvent)                                       //
         + n_committed * sizeof(PackedSlotOffset)                            //
         + ((n_committed > 0) ? sizeof(PackedArray<PackedSlotOffset>) : 0u)  //
         + (batt::make_copy(object.trimmed_prepare_jobs)                     //
            | batt::seq::map([](const TrimmedPrepareJob& pending) {
                return packed_sizeof(pending) + sizeof(PackedPointer<PackedTrimmedPrepareJob>);
              })  //
            | batt::seq::sum());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize packed_sizeof(const PackedVolumeTrimEvent& packed)
{
  return sizeof(PackedVolumeTrimEvent) +
         sizeof(PackedPointer<PackedTrimmedPrepareJob>) * packed.trimmed_prepare_jobs.size() +
         ([&]() -> usize {
           if (!packed.committed_jobs) {
             return 0;
           } else {
             return packed.committed_jobs->size() * sizeof(PackedSlotOffset);
           }
         }()) +
         (as_seq(packed.trimmed_prepare_jobs)  //
          | batt::seq::map([](const PackedPointer<PackedTrimmedPrepareJob>& p_job) {
              return packed_sizeof(*p_job);
            })  //
          | batt::seq::sum());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PackedVolumeTrimEvent* pack_object_to(const VolumeTrimEvent& object, PackedVolumeTrimEvent* packed,
                                      DataPacker* dst)
{
  packed->old_trim_pos = object.old_trim_pos;
  packed->new_trim_pos = object.new_trim_pos;
  packed->committed_jobs.offset = 0;
  packed->trimmed_prepare_jobs.initialize(0u);

  const usize pending_job_count = batt::make_copy(object.trimmed_prepare_jobs) | batt::seq::count();

  for (usize i = 0; i < pending_job_count; ++i) {
    if (dst->pack_record<PackedPointer<PackedTrimmedPrepareJob>>() == nullptr) {
      return nullptr;
    }
  }

  {
    bool error = false;
    batt::make_copy(object.trimmed_prepare_jobs)  //
        | batt::seq::for_each([&error, packed, dst](const TrimmedPrepareJob& job) {
            PackedTrimmedPrepareJob* const packed_job = pack_object(job, dst);
            if (packed_job == nullptr) {
              error = true;
              return batt::seq::LoopControl::kBreak;
            }
            packed->trimmed_prepare_jobs.item_count += 1;
            packed->trimmed_prepare_jobs[packed->trimmed_prepare_jobs.size() - 1].reset(packed_job,
                                                                                        dst);
            return batt::seq::LoopControl::kContinue;
          });

    if (error) {
      return nullptr;
    }
  }

  BATT_CHECK_EQ(pending_job_count, packed->trimmed_prepare_jobs.size());

  if ((batt::make_copy(object.committed_jobs) | batt::seq::take_n(1) | batt::seq::count()) > 0) {
    PackedArray<PackedSlotOffset>* const committed_jobs =
        dst->pack_record(batt::StaticType<PackedArray<PackedSlotOffset>>{});
    if (committed_jobs == nullptr) {
      return nullptr;
    }
    committed_jobs->initialize(0u);
    packed->committed_jobs.reset(committed_jobs, dst);

    bool error = false;
    batt::make_copy(object.committed_jobs)  //
        | batt::seq::for_each([&error, committed_jobs, dst](slot_offset_type prepare_slot) {
            if (!dst->pack_u64(prepare_slot)) {
              error = true;
              return batt::seq::LoopControl::kBreak;
            }
            committed_jobs->item_count += 1;
            return batt::seq::LoopControl::kContinue;
          });
    if (error) {
      return nullptr;
    }
  }

  return packed;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<VolumeTrimEvent> unpack_object(const PackedVolumeTrimEvent& packed, DataReader* src)
{
  return VolumeTrimEvent{
      .old_trim_pos = packed.old_trim_pos,
      .new_trim_pos = packed.new_trim_pos,

      .committed_jobs = [&]() -> batt::BoxedSeq<slot_offset_type> {
        if (packed.committed_jobs) {
          return as_seq(*packed.committed_jobs) |
                 batt::seq::map([](const PackedSlotOffset& prepare_slot) -> slot_offset_type {
                   return prepare_slot.value();
                 }) |
                 batt::seq::boxed();
        } else {
          return batt::seq::Empty<slot_offset_type>{} | batt::seq::boxed();
        }
      }(),

      .trimmed_prepare_jobs =
          as_seq(packed.trimmed_prepare_jobs)  //
          | batt::seq::map(
                [src](const PackedPointer<PackedTrimmedPrepareJob>& p_job) -> TrimmedPrepareJob {
                  StatusOr<TrimmedPrepareJob> job = unpack_object(*p_job, src);
                  BATT_CHECK_OK(job);
                  return std::move(*job);
                })  //
          | batt::seq::boxed(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status validate_packed_value(const PackedVolumeTrimEvent& packed, const void* buffer_data,
                             usize buffer_size)
{
  BATT_REQUIRE_OK(validate_packed_struct(packed, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(packed.committed_jobs, buffer_data, buffer_size));
  BATT_REQUIRE_OK(validate_packed_value(packed.trimmed_prepare_jobs, buffer_data, buffer_size));

  return batt::OkStatus();
}

}  // namespace llfs
