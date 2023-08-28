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
//
struct PackedVolumeIds {
  boost::uuids::uuid main_uuid;
  boost::uuids::uuid recycler_uuid;
  boost::uuids::uuid trimmer_uuid;
};

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeIds);

std::ostream& operator<<(std::ostream& out, const PackedVolumeIds& t);

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
      usize seed = 0;
      boost::hash_combine(seed, boost::hash<boost::uuids::uuid>{}(id.client));
      boost::hash_combine(seed, id.device.value());
      return seed;
    }
  };
};

bool operator==(const VolumeAttachmentId& l, const VolumeAttachmentId& r);

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
                      sizeof(boost::uuids::uuid) + sizeof(page_device_id_int) + sizeof(u64));

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeAttachEvent), 32);

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeAttachEvent);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeDetachEvent : PackedVolumeAttachmentEvent<PackedVolumeDetachEvent> {
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeDetachEvent),
                      sizeof(boost::uuids::uuid) + sizeof(page_device_id_int) + sizeof(u64));

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeDetachEvent), 32);

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeDetachEvent);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeRecovered {
};

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeRecovered);

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

/** \brief Calculates and returns the size (in bytes) of a PackedCommitJob for the passed
 * PrepareJob.  NOTE: this is *just* the size of the PackedCommitJob itself, not including
 * slot/variant headers.
 */
usize packed_sizeof_commit(const PrepareJob& obj);

/** \brief Calculates and returns the size (in bytes) of a full PackedCommitJob slot for the passed
 * PrepareJob.
 */
usize packed_sizeof_commit_slot(const PrepareJob& obj);

inline std::ostream& operator<<(std::ostream& out, const PrepareJob& t)
{
  return out << "PrepareJob{.new_page_ids=["
             << (batt::make_copy(t.new_page_ids) | batt::seq::count()) << "], .deleted_page_ids=["
             << (batt::make_copy(t.deleted_page_ids) | batt::seq::count())
             << "], .page_device_ids=[" << (batt::make_copy(t.page_device_ids) | batt::seq::count())
             << ", .user_data=...,}";
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Packed representation of PrepareJob.
//
// +------------------+----------------+--------------------------------------------+-- -
// | PackedPrepareJob | (user_data...) | PackedArray<PackedPageId> root_page_ids... | ...
// +------------------+----------------+--------------------------------------------+-- -
//
struct PackedPrepareJob {
  PackedPrepareJob(const PackedPrepareJob&) = delete;
  PackedPrepareJob& operator=(const PackedPrepareJob&) = delete;

  PackedPointer<PackedArray<PackedPageId>> root_page_ids;
  PackedPointer<PackedArray<PackedPageId>> new_page_ids;
  PackedPointer<PackedArray<PackedPageId>> deleted_page_ids;
  PackedPointer<PackedArray<little_page_device_id_int>> page_device_ids;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::string_view user_data() const noexcept
  {
    const char* p_begin = reinterpret_cast<const char*>(this + 1);
    const char* p_end = reinterpret_cast<const char*>(this->root_page_ids.get());

    BATT_CHECK_LE((const void*)p_begin, (const void*)p_end);

    return std::string_view{p_begin, usize(p_end - p_begin)};
  }
};

LLFS_DEFINE_PACKED_TYPE_FOR(PrepareJob, PackedPrepareJob);

usize packed_sizeof(const PackedPrepareJob& obj);

PackedPrepareJob* pack_object_to(const PrepareJob& obj, PackedPrepareJob* packed, DataPacker* dst);

StatusOr<Ref<const PackedPrepareJob>> unpack_object(const PackedPrepareJob& packed, DataReader*);

Status validate_packed_value(const PackedPrepareJob& packed, const void* buffer_data,
                             usize buffer_size);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct CommitJob {
  slot_offset_type prepare_slot_offset;
  const PackedPrepareJob* packed_prepare;
};

usize packed_sizeof(const CommitJob& obj);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
// +------------------+----------------+-------------------------------------------+
// | PackedCommitJob | (user_data...) | PackedArray<PackedPageId> root_page_ids... |
// +------------------+----------------+-------------------------------------------+
//
struct PackedCommitJob {
  PackedCommitJob(const PackedCommitJob&) = delete;
  PackedCommitJob& operator=(const PackedCommitJob&) = delete;

  PackedSlotOffset prepare_slot_offset;
  little_u32 prepare_slot_size;
  PackedPointer<PackedArray<PackedPageId>> root_page_ids;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::string_view user_data() const noexcept
  {
    const char* p_begin = reinterpret_cast<const char*>(this + 1);
    const char* p_end = reinterpret_cast<const char*>(this->root_page_ids.get());

    BATT_CHECK_LE((const void*)p_begin, (const void*)p_end);

    return std::string_view{p_begin, usize(p_end - p_begin)};
  }
};

LLFS_DEFINE_PACKED_TYPE_FOR(CommitJob, PackedCommitJob);

BATT_STATIC_ASSERT_EQ(sizeof(PackedCommitJob), 16);

usize packed_sizeof(const PackedCommitJob& obj);

PackedCommitJob* pack_object_to(const CommitJob& obj, PackedCommitJob* packed, DataPacker* dst);

StatusOr<Ref<const PackedCommitJob>> unpack_object(const PackedCommitJob& packed, DataReader*);

Status validate_packed_value(const PackedCommitJob& packed, const void* buffer_data,
                             usize buffer_size);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedRollbackJob {
  PackedSlotOffset prepare_slot;
};

LLFS_SIMPLE_PACKED_TYPE(PackedRollbackJob);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedVolumeFormatUpgrade {
  little_u64 new_version;
};

LLFS_SIMPLE_PACKED_TYPE(PackedVolumeFormatUpgrade);

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
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedVolumeTrimEvent), 16);

struct VolumeTrimEvent {
  slot_offset_type old_trim_pos;
  slot_offset_type new_trim_pos;
};

LLFS_DEFINE_PACKED_TYPE_FOR(VolumeTrimEvent, PackedVolumeTrimEvent);

usize packed_sizeof(const VolumeTrimEvent& object);

usize packed_sizeof(const PackedVolumeTrimEvent& packed);

PackedVolumeTrimEvent* pack_object_to(const VolumeTrimEvent& object, PackedVolumeTrimEvent* packed,
                                      DataPacker* dst);

StatusOr<VolumeTrimEvent> unpack_object(const PackedVolumeTrimEvent& packed, DataReader* src);

Status validate_packed_value(const PackedVolumeTrimEvent& packed, const void* buffer_data,
                             usize buffer_size);

std::ostream& operator<<(std::ostream& out, const VolumeTrimEvent& t);

}  // namespace llfs

#endif  // LLFS_VOLUME_EVENTS_HPP
