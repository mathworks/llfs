//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_LOG_SLOT_WRITER_HPP
#define LLFS_LOG_SLOT_WRITER_HPP

#include <llfs/log_device.hpp>

#include <llfs/data_layout.hpp>
#include <llfs/data_packer.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/mutex.hpp>
#include <batteries/async/types.hpp>
#include <batteries/suppress.hpp>

namespace llfs {

struct PackedRawData;

class SlotWriter
{
 public:
  class Append;

  explicit SlotWriter(LogDevice& log_device) noexcept;

  usize log_size() const
  {
    return this->log_device_.size();
  }

  usize log_capacity() const
  {
    return this->log_device_.capacity();
  }

  usize pool_size() const
  {
    return this->pool_.available();
  }

  usize in_use_size() const
  {
    return this->in_use_.size();
  }

  slot_offset_type slot_offset()
  {
    return this->log_writer_.lock().value()->slot_offset();
  }

  // Reserve `size` bytes in the log for future appends.
  //
  StatusOr<batt::Grant> reserve(u64 size, batt::WaitForResource wait_for_resource);

  // Set the new log trim offset (i.e., lower bound of the valid range); return the number of bytes
  // trimmed.
  //
  StatusOr<slot_offset_type> trim(slot_offset_type slot_lower_bound);

  // Set the new log trim offset (i.e., lower bound of the valid range); return a Grant of size
  // equal to the number of bytes trimmed; this Grant can be used to append new data to the log,
  // exactly like it was returned by `reserve`.
  //
  StatusOr<batt::Grant> trim_and_reserve(slot_offset_type slot_lower_bound);

  /** \brief Returns the current log device trim position.
   */
  slot_offset_type get_trim_pos() const noexcept
  {
    return this->log_device_.slot_range(LogReadMode::kSpeculative).lower_bound;
  }

  // Wait for the log to be trimmed to a point not-less-than `slot_lower_bound`.  NOTE: this does
  // *NOT* initiate a log trim, it merely blocks until the log's lower bound advances.
  //
  Status await_trim(slot_offset_type slot_lower_bound)
  {
    // It is safe to bypass the `log_writer_` Mutex in this case because we aren't doing anything
    // that modifies the log.
    //
    return this->log_device_.writer().await(SlotLowerBoundAt{.offset = slot_lower_bound});
  }

  // Shut down this object and all associated activities.
  //
  void halt();

  // Convenience; wait for data to sync to the log.
  //
  Status sync(LogReadMode mode, SlotUpperBoundAt event)
  {
    return this->log_device_.sync(mode, event);
  }

  // Prepare space in the log to append a slot.
  //
  StatusOr<Append> prepare(batt::Grant& grant, usize slot_body_size);

 private:
  LogDevice& log_device_;

  batt::Mutex<LogDevice::Writer*> log_writer_{&this->log_device_.writer()};

  // Initially the pool contains the entire log capacity; then we pull out a grant equal to the
  // "in-use" portion.
  //
  batt::Grant::Issuer pool_{this->log_device_.capacity()};

  // The `in_use_` grant will grow when we append to the log and will shrink when we trim.
  //
  batt::Grant in_use_{ok_result_or_panic(
      this->pool_.issue_grant(this->log_device_.size(), batt::WaitForResource::kFalse))};

  // The current trim lower bound for the log.
  //
  batt::Watch<slot_offset_type> trim_lower_bound_{
      log_device_.new_reader(/*slot_lower_bound=*/None, LogReadMode::kInconsistent)->slot_offset()};
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

class SlotWriter::Append
{
 public:
  explicit Append(SlotWriter* that, batt::Mutex<LogDevice::Writer*>::Lock writer_lock,
                  batt::Grant&& slot_grant, const MutableBuffer& slot_buffer,
                  usize slot_body_size) noexcept;

  Append(const Append&) = delete;
  Append& operator=(const Append&) = delete;

  BATT_SUPPRESS_IF_CLANG("-Wdefaulted-function-deleted")
  //
  Append(Append&&) = default;
  Append& operator=(Append&&) = default;
  //
  BATT_UNSUPPRESS_IF_CLANG()

  ~Append() noexcept;

  slot_offset_type slot_lower_bound() const
  {
    return this->slot_lower_bound_;
  }

  DataPacker& packer()
  {
    return this->packer_;
  }

  StatusOr<SlotRange> commit();

  void cancel();

 private:
  SlotWriter* that_;

  // To pack the data into the log, we need exclusive access.
  //
  batt::Mutex<LogDevice::Writer*>::Lock writer_lock_;

  // The slot_grant will be destroyed when we return, releasing its count back to the pool.
  //
  batt::Grant slot_grant_;

  // Was this append cancelled?
  //
  bool cancelled_;

  // Was this append committed?
  //
  bool committed_;

  // The beginning offset at which this append will occur if committed.
  //
  slot_offset_type slot_lower_bound_;

  // Exposed to the caller to serialize the contents of the slot.
  //
  DataPacker packer_;
};

inline usize packed_sizeof_slot_with_payload_size(usize payload_size)
{
  const usize slot_body_size = sizeof(PackedVariant<>) + payload_size;
  const usize slot_header_size = packed_sizeof_varint(slot_body_size);
  const usize slot_size = slot_header_size + slot_body_size;

  return slot_size;
}

template <typename T>
inline usize packed_sizeof_slot(const T& payload)
{
  return packed_sizeof_slot_with_payload_size(packed_sizeof(payload));
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
class TypedSlotWriter;

template <typename... Ts>
class TypedSlotWriter<PackedVariant<Ts...>> : public SlotWriter
{
 public:
  using Append = typename SlotWriter::Append;
  using SlotWriter::SlotWriter;

  struct NullPostCommitFn {
    using result_type = StatusOr<SlotRange>;

    result_type operator()(StatusOr<SlotRange> slot_range) const noexcept
    {
      return slot_range;
    }
  };

  /** \brief Appends `payload` to the log using the passed `caller_grant`.
   *
   * \param caller_grant Must be at least as large as packed_sizeof(payload)
   * \param payload The event data to append
   * \param post_commit_fn (StatusOr<SlotRange>(StatusOr<SlotRange>)) Called after the payload has
   *                       been committed to the log, while still holding the LogDevice::Writer
   *                       mutex; must return the passed slot_range (which is the interval where
   *                       `payload` was written)
   *
   * \return The slot offset range where `payload` was appended in the log
   */
  template <typename T, typename PostCommitFn = NullPostCommitFn>
  StatusOr<SlotRange> append(batt::Grant& caller_grant, T&& payload,
                             PostCommitFn&& post_commit_fn = {})
  {
    const usize slot_body_size = sizeof(PackedVariant<Ts...>) + packed_sizeof(payload);
    BATT_CHECK_NE(slot_body_size, 0u);

    StatusOr<Append> op = this->SlotWriter::prepare(caller_grant, slot_body_size);
    BATT_REQUIRE_OK(op);

    PackedVariant<Ts...>* variant_head =
        op->packer().pack_record(batt::StaticType<PackedVariant<Ts...>>{});
    if (!variant_head) {
      return ::llfs::make_status(StatusCode::kFailedToPackSlotVarHead);
    }

    variant_head->init(batt::StaticType<PackedTypeFor<T>>{});

    if (!pack_object(BATT_FORWARD(payload), &(op->packer()))) {
      return ::llfs::make_status(StatusCode::kFailedToPackSlotVarTail);
    }

    return post_commit_fn(op->commit());
  }
};

}  // namespace llfs

#endif  // LLFS_LOG_SLOT_WRITER_HPP
