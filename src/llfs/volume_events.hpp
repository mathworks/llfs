//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_EVENTS_HPP
#define LLFS_VOLUME_EVENTS_HPP

#include <llfs/appendable_job.hpp>
#include <llfs/packed_pointer.hpp>
#include <llfs/page_id_factory.hpp>
#include <llfs/page_layout.hpp>
#include <llfs/simple_packed_type.hpp>
#include <llfs/volume_events_fwd.hpp>

#include <batteries/bounds.hpp>
#include <batteries/static_assert.hpp>
#include <batteries/tuples.hpp>
#include <batteries/type_traits.hpp>

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

template <typename Derived>
struct PackedVolumeAttachmentEvent;

template <typename Derived>
std::ostream& operator<<(std::ostream& out, const PackedVolumeAttachmentEvent<Derived>& t);

struct VolumeAttachmentId {
  boost::uuids::uuid client;
  little_page_id_int device;

  struct Hash {
    u64 operator()(const VolumeAttachmentId& id) const
    {
      u64 seed = 0;
      boost::hash_combine(seed, boost::hash<boost::uuids::uuid>{}(id.client));
      boost::hash_combine(seed, id.device.value());
      return seed;
    }
  };
};

inline bool operator==(const VolumeAttachmentId& l, const VolumeAttachmentId& r)
{
  return l.client == r.client     //
         && l.device == r.device  //
      ;
}

std::ostream& operator<<(std::ostream& out, const VolumeAttachmentId& id);

BATT_EQUALITY_COMPARABLE((inline), VolumeAttachmentId, VolumeAttachmentId)

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename Derived>
struct PackedVolumeAttachmentEvent {
  VolumeAttachmentId id;
  little_u64 user_slot_offset;
};

template <typename Derived>
inline std::ostream& operator<<(std::ostream& out, const PackedVolumeAttachmentEvent<Derived>& t)
{
  return out << batt::name_of<Derived>() <<               //
         "{.client_uuid=" << t.id.client <<               //
         ", .device_id=" << t.id.device <<                //
         ", .user_slot_offset=" << t.user_slot_offset <<  //
         ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeAttachEvent : PackedVolumeAttachmentEvent<PackedVolumeAttachEvent> {
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeAttachEvent),
                      sizeof(boost::uuids::uuid) + sizeof(page_device_id_int) + 8);

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeAttachEvent), 32);

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeAttachEvent);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeDetachEvent : PackedVolumeAttachmentEvent<PackedVolumeDetachEvent> {
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeDetachEvent),
                      sizeof(boost::uuids::uuid) + sizeof(page_device_id_int) + 8);

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeDetachEvent), 32);

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeDetachEvent);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeIds {
  boost::uuids::uuid main_uuid;
  boost::uuids::uuid recycler_uuid;
  boost::uuids::uuid trimmer_uuid;
};

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeIds);

inline std::ostream& operator<<(std::ostream& out, const PackedVolumeIds& t)
{
  return out << "PackedVolumeIds{.main_uuid=" << t.main_uuid
             << ", .recycler_uuid=" << t.recycler_uuid << ", .trimmer_uuid=" << t.trimmer_uuid
             << ",}";
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedTrimmedPrepareJob {
  PackedSlotOffset prepare_slot;
  PackedArray<PackedPageId> page_ids;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedTrimmedPrepareJob), 16);

struct TrimmedPrepareJob {
  slot_offset_type prepare_slot;
  batt::BoxedSeq<PageId> page_ids;
};

LLFS_DEFINE_PACKED_TYPE_FOR(TrimmedPrepareJob, PackedTrimmedPrepareJob);

usize packed_sizeof(const TrimmedPrepareJob& object);

usize packed_sizeof(const PackedTrimmedPrepareJob& packed);

PackedTrimmedPrepareJob* pack_object_to(const TrimmedPrepareJob& object,
                                        PackedTrimmedPrepareJob* packed, DataPacker* dst);

StatusOr<TrimmedPrepareJob> unpack_object(const PackedTrimmedPrepareJob& packed, DataReader* src);

Status validate_packed_value(const PackedTrimmedPrepareJob& packed, const void* buffer_data,
                             usize buffer_size);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Written and flushed to the Volume WAL before trimming a segment of the log.  This allows
 * correct recovery in the case where a trim operation that needs to make page ref_count updates is
 * interrupted by shutdown.
 *
 * Only one pending PackedVolumeTrim event may be present in the WAL at a given time.  A trim event
 * is considered "pending" when new_trim_position is ahead of the actual log trim position, and it
 * is considered resolved when the actual log trim position catches up.
 */
struct PackedVolumeTrimEvent {
  PackedSlotOffset old_trim_pos;
  PackedSlotOffset new_trim_pos;

  /** \brief The prepare slot offsets of any jobs that were resolved in this trim (i.e., the
   * CommitJob slot for the corresponding prepare slot was found in the trimmed region).
   */
  PackedPointer<PackedArray<PackedSlotOffset>> committed_jobs;

  /** \brief The pending PrepareJob slots from this trimmed region or a previous one.
   */
  PackedArray<PackedPointer<PackedTrimmedPrepareJob>> trimmed_prepare_jobs;
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeTrimEvent), 28);

struct VolumeTrimEvent {
  slot_offset_type old_trim_pos;
  slot_offset_type new_trim_pos;
  batt::BoxedSeq<slot_offset_type> committed_jobs;
  batt::BoxedSeq<TrimmedPrepareJob> trimmed_prepare_jobs;
};

LLFS_DEFINE_PACKED_TYPE_FOR(VolumeTrimEvent, PackedVolumeTrimEvent);

usize packed_sizeof(const VolumeTrimEvent& object);

usize packed_sizeof(const PackedVolumeTrimEvent& packed);

PackedVolumeTrimEvent* pack_object_to(const VolumeTrimEvent& object, PackedVolumeTrimEvent* packed,
                                      DataPacker* dst);

StatusOr<VolumeTrimEvent> unpack_object(const PackedVolumeTrimEvent& packed, DataReader* src);

Status validate_packed_value(const PackedVolumeTrimEvent& packed, const void* buffer_data,
                             usize buffer_size);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeRecovered {
};

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeRecovered);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeFormatUpgrade {
  little_u64 new_version;
};

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeFormatUpgrade);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// The "prepare" phase slot written to a log when transactionally appending a PageCacheJob with user
// data (T).
//
struct PrepareJob {
  BoxedSeq<PageId> new_page_ids;
  BoxedSeq<PageId> deleted_page_ids;
  BoxedSeq<page_device_id_int> page_device_ids;
  PackableRef user_data;
};

usize packed_sizeof(const PrepareJob& obj);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Packed representation of PrepareJob.
//
struct PackedPrepareJob {
  PackedPrepareJob(const PackedPrepareJob&) = delete;
  PackedPrepareJob& operator=(const PackedPrepareJob&) = delete;

  u8 reserved_[sizeof(PackedVolumeTrimEvent) + sizeof(PackedPointer<PackedTrimmedPrepareJob>)];
  PackedPointer<PackedArray<PackedPageId>> new_page_ids;
  PackedPointer<PackedArray<PackedPageId>> deleted_page_ids;
  PackedPointer<PackedArray<PackedPageId>> root_page_ids;
  PackedPointer<PackedArray<little_page_device_id_int>> page_device_ids;
  PackedPointer<PackedRawData> user_data;
};

LLFS_DEFINE_PACKED_TYPE_FOR(PrepareJob, PackedPrepareJob);

usize packed_sizeof(const PackedPrepareJob& obj);

PackedPrepareJob* pack_object_to(const PrepareJob& obj, PackedPrepareJob* packed, DataPacker* dst);

StatusOr<Ref<const PackedPrepareJob>> unpack_object(const PackedPrepareJob& packed, DataReader*);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedCommitJob {
  u8 reserved_[sizeof(PackedVolumeTrimEvent) + sizeof(PackedArray<PackedSlotOffset>)];
  PackedSlotOffset prepare_slot;
};

LLFS_SIMPLE_PACKED_TYPE(PackedCommitJob);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedRollbackJob {
  PackedSlotOffset prepare_slot;
};

LLFS_SIMPLE_PACKED_TYPE(PackedRollbackJob);

}  // namespace llfs

#endif  // LLFS_VOLUME_EVENTS_HPP
