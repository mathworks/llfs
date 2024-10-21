//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_HPP
#define LLFS_PAGE_ALLOCATOR_HPP

#include <llfs/config.hpp>
//
#include <llfs/page_allocator_events.hpp>
#include <llfs/page_allocator_metrics.hpp>
#include <llfs/page_allocator_runtime_options.hpp>
#include <llfs/page_allocator_state.hpp>

#include <llfs/data_packer.hpp>
#include <llfs/log_device.hpp>
#include <llfs/metrics.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/slot_writer.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/mutex.hpp>
#include <batteries/async/runtime.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/async/types.hpp>
#include <batteries/async/watch.hpp>
#include <batteries/do_nothing.hpp>

#include <boost/functional/hash.hpp>
#include <boost/preprocessor/cat.hpp>
#include <boost/uuid/uuid.hpp>

#include <atomic>
#include <functional>
#include <memory>
#include <unordered_set>

namespace llfs {

namespace {
using ::batt::DoNothing;
}

class PageAllocator
{
 public:
  static constexpr usize kCheckpointGrantSize = 4 * kKiB;

  using Metrics = PageAllocatorMetrics;
  using State = PageAllocatorState;
  using StateNoLock = PageAllocatorStateNoLock;

  enum struct RecoveryStatus {
    kPending,
    kDone,
    kFailed,
  };

  static u64 calculate_log_size(u64 physical_page_count, u64 max_attachments);

  static StatusOr<std::unique_ptr<PageAllocator>> recover(
      const PageAllocatorRuntimeOptions& options, PageSize page_size, const PageIdFactory& page_ids,
      LogDeviceFactory& log_device_factory);

  static std::unique_ptr<PageAllocator> recover_or_die(const PageAllocatorRuntimeOptions& options,
                                                       PageSize page_size,
                                                       const PageIdFactory& page_ids,
                                                       LogDeviceFactory& log_device_factory)
  {
    auto result = PageAllocator::recover(options, page_size, page_ids, log_device_factory);
    BATT_CHECK(result.ok()) << result.status();
    return std::move(*result);
  }

  u64 max_attachments() const
  {
    return 64;  // TODO [tastolfi 2021-01-25]
  }

  ~PageAllocator() noexcept;

  void halt() noexcept;

  void join() noexcept;

  // Remove a page from the free pool but don't increment its refcount yet.
  //
  StatusOr<PageId> allocate_page(batt::WaitForResource wait_for_resource,
                                 const batt::CancelToken& cancel_token = batt::CancelToken::none());

  // Return the given page to the free pool without updating its refcount.
  //
  void deallocate_page(PageId id);

  /** \brief Called by attached users to indicate they have successfully recovered.
   */
  Status notify_user_recovered(const boost::uuids::uuid& user_id);

  // Return the current ref count value for a given page.
  //  TODO [tastolfi 2021-04-05] -- return generation too!
  //
  std::pair<i32, slot_offset_type> get_ref_count(PageId id);

  // Updates the index synchronously by applying the specified event.  Must be one of the event
  // types enumerated in page_device_event_types.hpp.
  // TODO [tastolfi 2023-03-22] deprecate public usage of this function!
  //
  template <typename T>
  StatusOr<slot_offset_type> update(T& event);

  // Updates the index synchronously by applying the specified event.  Must be one of the event
  // types enumerated in page_device_event_types.hpp.
  // TODO [tastolfi 2023-03-22] deprecate public usage of this function!
  //
  template <typename T>
  StatusOr<slot_offset_type> update_sync(T& event);

  // Block until updates have caught up with the specified slot number.
  //
  Status sync(slot_offset_type min_slot);

  /** \brief Atomically and durably update a set of page reference counts.
   *
   * Repeated calls to this function with the same `user_id` and `user_slot` (even across crashes)
   * are guaranteed to be idempotent.
   *
   * WARNING: if a given `page_id` appears more than once in the passed `ref_count_updates`
   * sequence, then only the **last** such item will have any effect on the PageAllocator state.
   * Callers of this function must combine the deltas for each page_id prior to passing the
   * sequence, if that is the desired behavior.
   */
  template <typename PageRefCountSeq, typename GarbageCollectFn = DoNothing>
  StatusOr<slot_offset_type> update_page_ref_counts(
      const boost::uuids::uuid& user_id, slot_offset_type user_slot,
      PageRefCountSeq&& ref_count_updates,
      GarbageCollectFn&& garbage_collect_fn = GarbageCollectFn{});

  /** \brief Polls the ref count for the given page_id every millisecond until it matches ref_count,
   * for a maximum of 10 seconds.
   *
   * If the timeout is reached, panic.
   *
   * \return true if the desired ref_count was observed, false if the given page generation was
   * advanced.
   */
  bool await_ref_count(PageId page_id, i32 ref_count);

  // Create an attachment used to detect duplicate change requests.
  //
  StatusOr<slot_offset_type> attach_user(const boost::uuids::uuid& user_id,
                                         slot_offset_type user_slot);

  // Remove a previously created attachment.
  //
  StatusOr<slot_offset_type> detach_user(const boost::uuids::uuid& user_id,
                                         slot_offset_type user_slot);

  // Returns a sequence of reference counts for all pages managed by this allocator.
  //
  BoxedSeq<PageRefCount> page_ref_counts();

  auto debug_info()
  {
    return [this](std::ostream& out) {
      out << "PageAllocator{.page_size=" << batt::dump_size(this->page_size())
          << ", .free=" << this->free_pool_size() << "/" << this->page_device_capacity() << ",}";
    };
  }

  u64 page_size() const
  {
    return this->page_size_;
  }

  u64 free_pool_size() const
  {
    return this->state_.no_lock().free_pool_size();
  }

  u64 page_device_capacity() const
  {
    return this->state_.no_lock().page_device_capacity();
  }

  page_device_id_int get_device_id() const
  {
    return this->state_.no_lock().page_ids().get_device_id();
  }

  u64 total_log_bytes_committed() const
  {
    return this->log_device_->slot_range(LogReadMode::kSpeculative).upper_bound;
  }

  u64 total_log_bytes_flushed() const
  {
    return this->log_device_->slot_range(LogReadMode::kDurable).upper_bound;
  }

  u64 total_log_bytes_trimmed() const
  {
    return this->log_device_->slot_range(LogReadMode::kDurable).lower_bound;
  }

  std::vector<PageAllocatorAttachmentStatus> get_all_clients_attachment_status() const;

  Optional<PageAllocatorAttachmentStatus> get_client_attachment_status(
      const boost::uuids::uuid& uuid) const;

  const Metrics& metrics() const
  {
    return this->metrics_;
  }

 private:  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  explicit PageAllocator(batt::TaskScheduler& scheduler, std::string_view name, PageSize page_size,
                         std::unique_ptr<LogDevice> log_device,
                         std::unique_ptr<State> recovered_state) noexcept;

  void learner_task_main();

  void checkpoint_task_main();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Metrics for this allocator.
  //
  Metrics metrics_;

  // For debug.
  //
  std::string name_;

  // The log for storing the index.
  //
  const std::unique_ptr<LogDevice> log_device_;

  // Make a thread-safe slot writer to append new events to the log.
  //
  TypedSlotWriter<PackedPageAllocatorEvent> slot_writer_{*this->log_device_};

  // The page size for the associated PageDevice.
  //
  const PageSize page_size_;

  // This is the target state for the PageAllocator; it is used to validate incoming proposals
  // (events) and determine which ones are invalid, valid but already processed, and valid in need
  // of processing.
  //
  batt::Mutex<std::unique_ptr<State>> state_;

  // Pool from which space is allocated to write checkpoint slices. MUST be declared before
  // checkpoint task so that its lifetime will bound that of the task.
  //
  batt::Grant checkpoint_grant_{
      ok_result_or_panic(this->slot_writer_.reserve(0, batt::WaitForResource::kFalse))};

  // Tracks the known flushed slot upper bound; this may trail the `durable_state_` learned upper
  // bound slightly, so on read, we must sync these two.
  //
  batt::Watch<slot_offset_type> durable_upper_bound_;

  // Flag that is set when we are stopping; so that checkpoint_task_main can tell the difference
  // between an expected fatal error and the other kind.
  //
  batt::Watch<bool> stop_requested_{false};

  // On recover, set to the number of attached users.  Attached users should call
  // `notify_user_recovered` to indicate that they are now in a clean state; when the last of these
  // happens, the PageAllocator changes from safe mode to normal mode.
  //
  batt::Watch<i64> recovering_user_count_;

  // Tracks the recovering users.
  //
  batt::Mutex<std::unordered_set<boost::uuids::uuid, boost::hash<boost::uuids::uuid>>>
      recovering_users_;

  // Writes checkpoint data so the log can be trimmed.
  //
  batt::Task checkpoint_task_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline StatusOr<slot_offset_type> PageAllocator::update(T& op)
{
  const usize slot_size = packed_sizeof_slot(op);
  const usize future_checkpoint_size = packed_sizeof_checkpoint(op);
  const usize reserve_size = slot_size + future_checkpoint_size;

  BATT_DEBUG_INFO("PageAllocator::update_sync waiting to reserve "
                  << reserve_size << ", checkpoint_grant=" << this->checkpoint_grant_
                  << ", slot_pool_size=" << this->slot_writer_.pool_size()
                  << ", slot_in_use=" << this->slot_writer_.in_use_size());

  StatusOr<batt::Grant> slot_grant =
      this->slot_writer_.reserve(reserve_size, batt::WaitForResource::kTrue);
  BATT_REQUIRE_OK(slot_grant);

  // The slot offset range of the committed event; updated in the lock block below.
  //
  StatusOr<SlotRange> commit_slot;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    auto locked_state = this->state_.lock();
    State* state = locked_state->get();

    // See if the operation is valid; we pass by pointer because `propose` can modify the passed
    // data to include additional information needed by `learn` below.
    //
    const State::ProposalStatus proposal_status = state->propose(&op);

    if (proposal_status == State::ProposalStatus::kNoChange) {
      return this->log_device_->slot_range(LogReadMode::kDurable).upper_bound;
      //
      // TODO [tastolfi 2021-03-08] - should we attempt to wait until the operation is durable??

    } else if (proposal_status == State::ProposalStatus::kInvalid_NotAttached) {
      return ::llfs::make_status(StatusCode::kPageAllocatorNotAttached);

    } else if (proposal_status == State::ProposalStatus::kInvalid_OutOfAttachments) {
      return ::llfs::make_status(StatusCode::kOutOfAttachments);
    }

    BATT_CHECK_EQ(proposal_status, State::ProposalStatus::kValid);

    commit_slot = this->slot_writer_.append(*slot_grant, op);
    BATT_REQUIRE_OK(commit_slot);

    // Apply the event to the state machine.
    //
    state->learn(*commit_slot, op, this->metrics_);
  }
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  // Take whatever was left over from the slot grant and allow it to be used for checkpoints.
  //
  this->checkpoint_grant_.subsume(std::move(*slot_grant));

  return commit_slot->upper_bound;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename T>
inline StatusOr<slot_offset_type> PageAllocator::update_sync(T& op)
{
  StatusOr<slot_offset_type> commit_slot = this->update(op);
  BATT_REQUIRE_OK(commit_slot);

  Status sync_status = this->sync(*commit_slot);
  BATT_REQUIRE_OK(sync_status);

  return commit_slot;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename PageRefCountSeq, typename GarbageCollectFn>
inline StatusOr<slot_offset_type> PageAllocator::update_page_ref_counts(
    const boost::uuids::uuid& user_id, slot_offset_type user_slot,
    PageRefCountSeq&& ref_count_updates, GarbageCollectFn&& garbage_collect_fn)
{
  const std::size_t op_size =
      packed_sizeof_page_allocator_txn(batt::make_copy(ref_count_updates) | seq::count());

  std::unique_ptr<u8[]> buffer{new u8[op_size]};
  DataPacker packer{MutableBuffer{buffer.get(), op_size}};
  auto* txn = packer.pack_record<PackedPageAllocatorTxn>();
  BATT_CHECK_NOT_NULLPTR(txn);

  txn->user_slot.user_id = user_id;
  txn->user_slot.slot_offset = user_slot;
  txn->user_index = PageAllocatorState::kInvalidUserIndex;
  txn->ref_counts.initialize(0u);

  BasicArrayPacker<PackedPageRefCount, DataPacker> packed_ref_counts{&txn->ref_counts, &packer};
  batt::make_copy(ref_count_updates) | seq::for_each([&packed_ref_counts](const PageRefCount& prc) {
    BATT_CHECK(packed_ref_counts.pack_item(prc));
  });

  LLFS_VLOG(2) << "updating ref counts: " << batt::dump_range(txn->ref_counts, batt::Pretty::True);

  static std::atomic<usize> sample_count{0};
  static std::atomic<usize> prc_count{0};

  sample_count.fetch_add(1);
  prc_count.fetch_add(txn->ref_counts.size());

  LLFS_LOG_INFO_EVERY_T(5.0 /*seconds*/)
      << "Average pages per allocator update: "
      << ((double)prc_count.load() / (double)sample_count.load());

  StatusOr<slot_offset_type> update_status = this->update(*txn);
  BATT_REQUIRE_OK(update_status);

  {
    auto& state = this->state_.no_lock();

    BATT_FORWARD(ref_count_updates) |

        // Select only the pages from this set of updates that are now at ref_count==1, meaning
        // they are ready for GC.  If ref_count is 1, that means the current call has removed the
        // last reference to a page, so we can be sure there are no race conditions.
        //
        seq::filter_map([&state, txn](const PageRefCount& delta) -> Optional<PageId> {
          if (delta.ref_count < 0 && delta.ref_count != kRefCount_1_to_0) {
            const PageAllocatorRefCountStatus page_status =
                state.get_ref_count_status(delta.page_id);

            if (page_status.ref_count == 1 &&                 //
                page_status.user_index == txn->user_index &&  //
                page_status.page_id == delta.page_id) {
              BATT_CHECK_EQ(PageIdFactory::get_device_id(delta.page_id),
                            state.page_ids().get_device_id());
              return delta.page_id;
            }
          }
          return None;
        }) |

        // Call the user-supplied function.
        //
        seq::for_each(BATT_FORWARD(garbage_collect_fn));
  }

  LLFS_VLOG(2) << [&](std::ostream& out) {
    auto& state = this->state_.no_lock();
    for (const auto& prc : txn->ref_counts) {
      out << prc << " -> " << state.get_ref_count_status(prc.page_id.unpack()).ref_count << ", ";
    }
  };

  return update_status;
}

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_HPP
