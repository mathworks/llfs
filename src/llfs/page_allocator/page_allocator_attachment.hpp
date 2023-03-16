//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_ATTACHMENT_HPP
#define LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_ATTACHMENT_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_allocator/page_allocator_object_base.hpp>

#include <boost/functional/hash.hpp>
#include <boost/uuid/uuid.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An attachment from a unique user to the index; facilitates idempotent ("exactly-once")
 * updates.
 */
class PageAllocatorAttachment : public PageAllocatorObjectBase
{
 public:
  /** \brief Create a new attachment setting the initial values of user id and slot.
   */
  explicit PageAllocatorAttachment(const boost::uuids::uuid& user_id,
                                   slot_offset_type user_slot) noexcept
      : user_id_{user_id}
      , user_slot_{user_slot}
  {
  }

  /** \brief Returns the user id bound to this attachment.
   */
  const boost::uuids::uuid& get_user_id() const noexcept
  {
    return this->user_id_;
  }

  /** \brief Updates the user slot associated with this attachment.
   */
  void set_user_slot(slot_offset_type slot_offset) noexcept
  {
    this->user_slot_ = slot_offset;
  }

  /** \brief Returns the current slot offset associated with this attachment.
   */
  slot_offset_type get_user_slot() const noexcept
  {
    return this->user_slot_;
  }

 private:
  const boost::uuids::uuid user_id_;
  slot_offset_type user_slot_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief Used to report attachment status to clients of PageAllocator.
 */
struct PageAllocatorAttachmentStatus {
  boost::uuids::uuid user_id;
  slot_offset_type user_slot;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief (Hash) Map from client/user UUID to PageAllocatorAttachment.
 */
using PageAllocatorAttachmentMap =
    std::unordered_map<boost::uuids::uuid, std::unique_ptr<PageAllocatorAttachment>,
                       boost::hash<boost::uuids::uuid>>;

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_PAGE_ALLOCATOR_ATTACHMENT_HPP
