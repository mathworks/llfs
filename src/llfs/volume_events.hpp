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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename Derived>
struct PackedVolumeAttachmentEvent {
  boost::uuids::uuid client_uuid;
  little_page_id_int device_id;
  little_u64 user_slot_offset;

  struct Hash {
    u64 operator()(const PackedVolumeAttachmentEvent& event) const
    {
      u64 seed = 0;
      boost::hash_combine(seed, boost::hash<boost::uuids::uuid>{}(event.client_uuid));
      boost::hash_combine(seed, event.device_id.value());
      /* user_slot_offset intentionally omitted */
      return seed;
    }
  };
};

template <typename Derived>
inline std::ostream& operator<<(std::ostream& out, const PackedVolumeAttachmentEvent<Derived>& t)
{
  return out << batt::name_of<Derived>() <<               //
         "{.client_uuid=" << t.client_uuid <<             //
         ", .device_id=" << t.device_id <<                //
         ", .user_slot_offset=" << t.user_slot_offset <<  //
         ",}";
}

template <typename Derived>
inline bool operator==(const PackedVolumeAttachmentEvent<Derived>& l,
                       const PackedVolumeAttachmentEvent<Derived>& r)
{
  return l.client_uuid == r.client_uuid  //
         && l.device_id == r.device_id   //
      /* user_slot_offset intentionally omitted */;
}

BATT_EQUALITY_COMPARABLE((template <typename Derived> inline), PackedVolumeAttachmentEvent<Derived>,
                         PackedVolumeAttachmentEvent<Derived>)

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
  PackableRef user_data;
};

usize packed_sizeof(const PrepareJob& obj);

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
// Packed representation of PrepareJob.
//
struct PackedPrepareJob {
  PackedPrepareJob(const PackedPrepareJob&) = delete;
  PackedPrepareJob& operator=(const PackedPrepareJob&) = delete;

  PackedPointer<PackedArray<PackedPageId>> new_page_ids;
  PackedPointer<PackedArray<PackedPageId>> deleted_page_ids;
  PackedPointer<PackedArray<PackedPageId>> root_page_ids;
  PackedPointer<PackedRawData> user_data;
};

LLFS_DEFINE_PACKED_TYPE_FOR(PrepareJob, PackedPrepareJob);

usize packed_sizeof(const PackedPrepareJob& obj);

PackedPrepareJob* pack_object_to(const PrepareJob& obj, PackedPrepareJob* packed, DataPacker* dst);

StatusOr<Ref<const PackedPrepareJob>> unpack_object(const PackedPrepareJob& packed, DataReader*);

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
struct PackedCommitJob {
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
