//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_VOLUME_MULTI_APPEND_HPP
#define LLFS_VOLUME_MULTI_APPEND_HPP

#ifndef LLFS_VOLUME_HPP
#error This file must be included from <llfs/volume.hpp>!
#endif

#include <llfs/slot_writer.hpp>

namespace llfs {

/** \brief An atomic, multiple slot append operation for a Volume.
 *
 * Unlike regular sequential appends, VolumeMultiAppend offers the following guarantees:
 *
 *  - All slots in the multi-append will appear sequentially with no interposed slots (from other
 *    writers/threads)
 *  - Only a single call will be made to the underlying LogDevice::Writer to commit the data
 *  - On crash/recovery, either all of the slots in the multi-append will be visible, or none
 */
class VolumeMultiAppend
{
 public:
  /** \brief Returns the size grant required for a multi-append, given the total size of the grants
   * needed for each slot.
   */
  static constexpr u64 calculate_grant_size(u64 slots_total_size) noexcept
  {
    return slots_total_size + SlotWriter::WriterLock::kBeginAtomicRangeTokenSize +
           SlotWriter::WriterLock::kEndAtomicRangeTokenSize;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Locks the volume's slot writer and initiates an atomic multi-slot append operation.
   */
  explicit VolumeMultiAppend(Volume& volume) noexcept : op_{*volume.slot_writer_}
  {
  }

  /** \brief VolumeMultiAppend is not copy-constructible.
   */
  VolumeMultiAppend(const VolumeMultiAppend&) = delete;

  /** \brief VolumeMultiAppend is not copy-assignable.
   */
  VolumeMultiAppend& operator=(const VolumeMultiAppend&) = delete;

  /** \brief Destroys the object; panics if neither of this->commit() nor this->cancel() have been
   * invoked.
   */
  ~VolumeMultiAppend() noexcept
  {
    BATT_CHECK(this->closed_);
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns true iff the multi-append operation is still accepting new slots.  Calling
   * this->commit or this->cancel the first time will cause the status of the operation to go from
   * "open" to "closed" (not open).
   */
  bool is_open() const noexcept
  {
    return !this->closed_;
  }

  /** \brief Adds a single slot (the packed representation of T) to the end of the multi-append. The
   * passed grant must be large enough to cover the *entire* multi-append, including the current
   * payload and all payloads previously passed to append.
   *
   * \return the slot offset interval of the appended data
   */
  template <typename T>
  StatusOr<SlotRange> append(const T& payload, const batt::Grant& grant) noexcept
  {
    BATT_CHECK(!this->closed_);

    if (this->first_) {
      this->first_ = false;
      BATT_REQUIRE_OK(this->op_.begin_atomic_range(grant));
    }

    llfs::PackObjectAsRawData<const T&> packed_obj_as_raw{payload};

    return this->op_.append(grant, packed_obj_as_raw);
  }

  /** \brief Adds a single slot (raw bytes) to the multi-append.
   */
  StatusOr<SlotRange> append(const std::string_view& payload, const batt::Grant& grant)
  {
    return this->append(pack_as_raw(payload), grant);
  }

  /** \brief Attempts to commit all slots that have been added to the multi-append, consuming part
   * or all of the passed grant.
   *
   * This operation may fail due to insufficient grant or I/O errors in the underlying LogDevice
   * (even though the data is flushed from memory to storage asychronously; this means an I/O error
   * reported here was likely encountered while flushing previous data).
   *
   * \return the entire committed slot interval for the slots in the multi-append
   */
  StatusOr<SlotRange> commit(batt::Grant& grant) noexcept
  {
    BATT_CHECK(!this->closed_);

    if (!this->first_) {
      BATT_REQUIRE_OK(this->op_.end_atomic_range(grant));
    }

    StatusOr<SlotRange> result = this->op_.finalize(grant);
    BATT_REQUIRE_OK(result);

    this->closed_ = true;
    return result;
  }

  /** \brief Abandons the multi-append without modifying the Volume root log.
   *
   * Note: does not release the writer lock for the volume; this object must be destroyed to release
   * the lock.
   */
  void cancel() noexcept
  {
    BATT_CHECK(!this->closed_);
    this->closed_ = true;
  }

 private:
  // Implements the actual log appending.
  //
  TypedSlotWriter<VolumeEventVariant>::MultiAppend op_;

  // true iff no call append has yet been made; used to determine when we should write a begin
  // atomic range token to the log.
  //
  bool first_ = true;

  // true iff commit or cancel has been called.
  //
  bool closed_ = false;
};

}  //namespace llfs

#endif  // LLFS_VOLUME_MULTI_APPEND_HPP
