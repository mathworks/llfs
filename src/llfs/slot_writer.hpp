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
#include <llfs/optional.hpp>
#include <llfs/slot_parse.hpp>

#include <batteries/async/grant.hpp>
#include <batteries/async/mutex.hpp>
#include <batteries/async/types.hpp>
#include <batteries/suppress.hpp>

namespace llfs {

struct PackedRawData;

class SlotWriter
{
 public:
  using Self = SlotWriter;

  class WriterLock;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit SlotWriter(LogDevice& log_device) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

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

class SlotWriter::WriterLock
{
 public:
  using Self = WriterLock;

  static constexpr usize kBeginAtomicRangeTokenSize = 3;
  static constexpr usize kEndAtomicRangeTokenSize = 2;

  static Slice<const u8> begin_atomic_range_token() noexcept;
  static Slice<const u8> end_atomic_range_token() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Acquires an exclusive lock on writing to the log managed by `slot_writer`, preparing
   * to write new slot data.  This lock is released when the WriterLock object goes out of scope.
   */
  explicit WriterLock(SlotWriter& slot_writer) noexcept;

  /** \brief WriterLock is not copyable.
   */
  WriterLock(const WriterLock&) = delete;

  /** \brief WriterLock is not copyable.
   */
  WriterLock& operator=(const WriterLock&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Appends special token to the log to indicate the start of a sequence of slots that
   * must commit/recover atomically.
   *
   * The token commit is deferred, requiring later calls to end_atomic_range and commit.
   */
  Status begin_atomic_range(const batt::Grant& caller_grant) noexcept;

  /** \brief Appends special sequence to the log indicating the end of the current atomic range.
   *
   * The token commit is deferred, requiring a later call to commit.
   */
  Status end_atomic_range(const batt::Grant& caller_grant) noexcept;

  /** \brief Verifies that the passed grant can accomodate a new slot with the passed payload size
   * (in addition to any currently deferred slots), then allocates space in the log and writes a
   * header for the new slot, returning a mutable buffer for _just_ the payload portion.
   */
  StatusOr<MutableBuffer> prepare(usize slot_payload_size,
                                  const batt::Grant& caller_grant) noexcept;

  /** \brief Defers the currently prepared slot for later commit.
   *
   * Future calls to prepare will return a memory segment after the deferred commit slots.
   */
  SlotRange defer_commit() noexcept;

  /** \brief Commits the current prepared slot and any deferred commit slots to the log.
   *
   * Transfers the size of the committed data from `caller_grant` to the SlotWriter's in_use_
   * grant.
   *
   * \return the log slot range of the committed data.
   */
  StatusOr<SlotRange> commit(batt::Grant& caller_grant) noexcept;

  /** \brief Reverts the effect of the most recent call to prepare after the most recent call to
   * defer_commit or commit.
   */
  void cancel_prepare() noexcept;

  /** \brief Equivalent to this->cancel_prepare() plus clearing out all deferred commits.  Rolls
   * back everything since the most recent call to this->commit().
   */
  void cancel_all() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  Status append_token_impl(const Slice<const u8>& token, const batt::Grant& caller_grant) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The SlotWriter object passed in at construction time.
   */
  SlotWriter& slot_writer_;

  /** \brief Lock on the SlotWriter's LogDevice::Writer.
   */
  batt::ScopedLock<LogDevice::Writer*> writer_lock_;

  /** \brief The size (bytes) of the most recently prepared slot buffer (allocated via
   * this->prepare).  This size includes the varint slot header (the payload size), so it is
   * always larger than the `slot_payload_size` arg passed to `this->prepare()`.
   */
  usize prepare_size_ = 0;

  /** \brief The size (bytes) of all slots that have been fully packed and are ready for commit
   * the next time `this->commit()` is called.
   */
  usize deferred_commit_size_ = 0;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

inline constexpr usize packed_sizeof_slot_with_payload_size(usize payload_size)
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
  using SlotWriter::SlotWriter;

  struct NullPostCommitFn {
    using result_type = StatusOr<SlotRange>;

    result_type operator()(StatusOr<SlotRange> slot_range) const noexcept
    {
      return slot_range;
    }
  };

  /** \brief An append operation of one or more slots.
   */
  class MultiAppend
  {
   public:
    /** \brief Initialize a MultiAppend operation using the passed TypedSlotWriter.
     *
     * This will obtain a lock on the slot writer's LogDevice::Writer mutex.
     */
    explicit MultiAppend(TypedSlotWriter& slot_writer) noexcept : writer_lock_{slot_writer}
    {
    }

    MultiAppend(const MultiAppend&) = delete;
    MultiAppend& operator=(const MultiAppend&) = delete;

    //----- --- -- -  -  -   -

    Status begin_atomic_range(const batt::Grant& caller_grant) noexcept
    {
      return this->writer_lock_.begin_atomic_range(caller_grant);
    }

    Status end_atomic_range(const batt::Grant& caller_grant) noexcept
    {
      return this->writer_lock_.end_atomic_range(caller_grant);
    }

    /** \brief Packs a single slot into the log device buffer.  Does not commit the slot; all slots
     * are committed atomically when this->finalize() is called.
     */
    template <typename T, typename PackedT = PackedTypeFor<T>>
    StatusOr<SlotParseWithPayload<const PackedT*>> typed_append(const batt::Grant& caller_grant,
                                                                T&& payload)
    {
      // Calculate packed size of payload.
      //
      const usize slot_payload_size = sizeof(PackedVariant<Ts...>) + packed_sizeof(payload);

      // Allocate log buffer space and write the slot header (varint).
      //
      BATT_ASSIGN_OK_RESULT(MutableBuffer payload_buffer,
                            this->writer_lock_.prepare(slot_payload_size, caller_grant));

      // Pack the payload.
      //
      DataPacker packer{payload_buffer};

      PackedVariant<Ts...>* const variant_head =
          packer.pack_record(batt::StaticType<PackedVariant<Ts...>>{});

      if (!variant_head) {
        return ::llfs::make_status(StatusCode::kFailedToPackSlotVarHead);
      }

      variant_head->init(batt::StaticType<PackedT>{});

      if (!pack_object(BATT_FORWARD(payload), &packer)) {
        return ::llfs::make_status(StatusCode::kFailedToPackSlotVarTail);
      }

      // Add the packed slot to the deferred commit segment.
      //
      SlotRange slot_range = this->writer_lock_.defer_commit();
      auto* slot_payload_start = reinterpret_cast<const char*>(variant_head);

      return SlotParseWithPayload<const PackedT*>{
          .slot =
              SlotParse{
                  .offset = slot_range,
                  .body = std::string_view{slot_payload_start, slot_payload_size},
                  .total_grant_spent = slot_payload_size,
              },
          .payload = variant_head->as(batt::StaticType<PackedT>{}),
      };
    }

    /** \brief Like typed_append, but erases type information from the returned value.
     */
    template <typename T, typename PackedT = PackedTypeFor<T>>
    StatusOr<SlotRange> append(const batt::Grant& caller_grant, T&& payload)
    {
      StatusOr<SlotParseWithPayload<const PackedTypeFor<T>*>> packed =
          this->typed_append(caller_grant, BATT_FORWARD(payload));

      BATT_REQUIRE_OK(packed);

      return {packed->slot.offset};
    }

    /** \brief Atomically commits all slots appended previously by `this` via typed_append.
     */
    template <typename PostCommitFn = NullPostCommitFn>
    StatusOr<SlotRange> finalize(batt::Grant& caller_grant,
                                 PostCommitFn&& post_commit_fn = {}) noexcept
    {
      return post_commit_fn(this->writer_lock_.commit(caller_grant));
    }

    //----- --- -- -  -  -   -
   private:
    SlotWriter::WriterLock writer_lock_;
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
   * \return The SlotParse and pointer to packed variant case.
   */
  template <typename T, typename PackedT = PackedTypeFor<T>,
            typename PostCommitFn = NullPostCommitFn>
  StatusOr<SlotParseWithPayload<const PackedT*>> typed_append(batt::Grant& caller_grant,
                                                              T&& payload,
                                                              PostCommitFn&& post_commit_fn = {})
  {
    // Appending a single slot is treated as a degenerate case of a MultiAppend.
    //
    MultiAppend op{*this};

    StatusOr<SlotParseWithPayload<const PackedT*>> result =
        op.typed_append(caller_grant, BATT_FORWARD(payload));

    BATT_REQUIRE_OK(result);
    BATT_REQUIRE_OK(op.finalize(caller_grant, post_commit_fn));

    return result;
  }

  template <typename T, typename PostCommitFn = NullPostCommitFn>
  StatusOr<SlotRange> append(batt::Grant& caller_grant, T&& payload,
                             PostCommitFn&& post_commit_fn = {})
  {
    StatusOr<SlotParseWithPayload<const PackedTypeFor<T>*>> packed =
        this->typed_append(caller_grant, BATT_FORWARD(payload), BATT_FORWARD(post_commit_fn));

    BATT_REQUIRE_OK(packed);

    return {packed->slot.offset};
  }
};

}  // namespace llfs

#endif  // LLFS_LOG_SLOT_WRITER_HPP
