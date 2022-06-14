#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_HPP
#define LLFS_PAGE_ALLOCATOR_HPP

#include <llfs/data_packer.hpp>
#include <llfs/log_device.hpp>
#include <llfs/metrics.hpp>
#include <llfs/page_allocator_events.hpp>
#include <llfs/page_allocator_metrics.hpp>
#include <llfs/page_allocator_state.hpp>
#include <llfs/slot_reader.hpp>
#include <llfs/slot_writer.hpp>

#include <turtle/util/do_nothing.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/runtime.hpp>
#include <batteries/async/task_scheduler.hpp>
#include <batteries/async/types.hpp>
#include <batteries/async/watch.hpp>

#include <boost/functional/hash.hpp>
#include <boost/preprocessor/cat.hpp>
#include <boost/uuid/uuid.hpp>

#include <atomic>
#include <functional>
#include <memory>

namespace llfs {

namespace {
using ::turtle_db::DoNothing;
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

  static StatusOr<std::unique_ptr<PageAllocator>> recover(batt::TaskScheduler& scheduler,
                                                          std::string_view name,
                                                          const PageIdFactory& page_ids,
                                                          LogDeviceFactory& log_device_factory);

  static std::unique_ptr<PageAllocator> recover_or_die(batt::TaskScheduler& scheduler,
                                                       std::string_view name,
                                                       const PageIdFactory& page_ids,
                                                       LogDeviceFactory& log_device_factory)
  {
    auto result = PageAllocator::recover(scheduler, name, page_ids, log_device_factory);
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
  StatusOr<PageId> allocate_page(batt::WaitForResource wait_for_resource);

  // Return the given page to the free pool without updating its refcount.
  //
  void deallocate_page(PageId id);

  // Removes page from the free pool in order to recover a pending job.
  //
  Status recover_page(PageId id);

  // Return the current ref count value for a given page.
  //  TODO [tastolfi 2021-04-05] -- return generation too!
  //
  std::pair<i32, slot_offset_type> get_ref_count(PageId id);

  // Updates the index synchronously by applying the specified event.  Must be one of the event
  // types enumerated in page_device_event_types.hpp.
  //
  template <typename T>
  StatusOr<slot_offset_type> update(const T& event);

  // Updates the index synchronously by applying the specified event.  Must be one of the event
  // types enumerated in page_device_event_types.hpp.
  //
  template <typename T>
  StatusOr<slot_offset_type> update_sync(const T& event);

  // Block until updates have caught up with the specified slot number.
  //
  Status sync(slot_offset_type min_slot);

  // Atomically and durably update a set of page reference counts.  Repeated calls to this function
  // with the same `user_id` and `user_slot` (even across crashes) are guaranteed to be idempotent.
  //
  template <typename PageRefCountSeq, typename GarbageCollectFn = DoNothing>
  StatusOr<slot_offset_type> update_page_ref_counts(
      const boost::uuids::uuid& user_id, slot_offset_type user_slot,
      PageRefCountSeq&& ref_count_updates,
      GarbageCollectFn&& garbage_collect_fn = GarbageCollectFn{})
  {
    const std::size_t op_size =
        packed_sizeof_page_allocator_txn(batt::make_copy(ref_count_updates) | seq::count());

    std::unique_ptr<u8[]> buffer{new u8[op_size]};
    DataPacker packer{MutableBuffer{buffer.get(), op_size}};
    auto* txn = packer.pack_record<PackedPageAllocatorTxn>();
    BATT_CHECK_NOT_NULLPTR(txn);

    txn->user_slot.user_id = user_id;
    txn->user_slot.slot_offset = user_slot;
    txn->ref_counts.initialize(0u);

    BasicArrayPacker<PackedPageRefCount, DataPacker> packed_ref_counts{&txn->ref_counts, &packer};
    batt::make_copy(ref_count_updates) |
        seq::for_each([&packed_ref_counts](const PageRefCount& prc) {
          BATT_CHECK(packed_ref_counts.pack_item(prc));
        });

    VLOG(2) << "updating ref counts: " << batt::dump_range(txn->ref_counts, batt::Pretty::True);

    StatusOr<slot_offset_type> update_status = this->update(*txn);
    BATT_REQUIRE_OK(update_status);

    {
      auto& state = this->state_.no_lock();

      BATT_FORWARD(ref_count_updates) |

          // Notify any tasks awaiting updates for the passed page ids.
          //
          seq::inspect([](const PageRefCount& prc) {
            batt::Runtime::instance().notify(prc.page_id);
          }) |

          // Select only the pages from this set of updates that are now at ref_count==1, meaning
          // they are ready for GC.  If ref_count is 1, that means the current call has removed the
          // last reference to a page, so we can be sure there are no race conditions.
          //
          seq::filter_map([&state](const PageRefCount& prc) -> Optional<page_id_int> {
            BATT_CHECK_EQ(PageIdFactory::get_device_id(PageId{prc.page_id}),
                          state.page_ids().get_device_id());
            const auto info = state.get_ref_count_obj(PageId{prc.page_id});
            if (info.ref_count == 1 && info.page_id == prc.page_id) {
              return {prc.page_id};
            }
            return None;
          }) |

          // Call the user-supplied function.
          //
          seq::for_each(BATT_FORWARD(garbage_collect_fn));
    }

    VLOG(2) << [&](std::ostream& out) {
      auto& state = this->state_.no_lock();
      for (const auto& prc : txn->ref_counts) {
        out << prc << " -> " << state.get_ref_count_obj(PageId{prc.page_id}).ref_count;
      }
    };

    return update_status;
  }

  bool await_ref_count(PageId page_id, i32 ref_count)
  {
    return batt::Runtime::instance().await_condition(
        [this, ref_count](PageId page_id) -> bool {
          return this->get_ref_count(page_id).first == ref_count;
        },
        page_id);
  }

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
      out << "{" << this->state_.no_lock().free_pool_size() << "/"
          << this->state_.no_lock().page_device_capacity() << "}";
    };
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
  explicit PageAllocator(batt::TaskScheduler& scheduler, std::string_view name,
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

  // Writes checkpoint data so the log can be trimmed.
  //
  batt::Task checkpoint_task_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
template <typename T>
inline StatusOr<slot_offset_type> PageAllocator::update(const T& op)
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

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    auto locked_state = this->state_.lock();
    State* speculative = locked_state->get();

    const State::ProposalStatus proposal_status = speculative->propose(op);

    if (proposal_status == State::ProposalStatus::kNoChange) {
      return this->log_device_->slot_range(LogReadMode::kDurable).upper_bound;
      //
      // TODO [tastolfi 2021-03-08] - should we attempt to wait until the operation is durable??
    } else if (proposal_status == State::ProposalStatus::kInvalid_NotAttached) {
      return Status{batt::StatusCode::kInvalidArgument};
      // TODO [tastolfi 2021-10-20] "Update can not be processed; the client must attach to the page
      // allocator"
    }

    BATT_CHECK_EQ(proposal_status, State::ProposalStatus::kValid);

    commit_slot = this->slot_writer_.append(*slot_grant, op);
    BATT_REQUIRE_OK(commit_slot);

    // Apply the event to the state machine.
    //
    speculative->learn(commit_slot->lower_bound, op, this->metrics_);
  }
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  // Take whatever was left over from the slot grant and allow it to be used for checkpoints.
  //
  this->checkpoint_grant_.subsume(std::move(*slot_grant));

  return commit_slot->upper_bound;
}

template <typename T>
inline StatusOr<slot_offset_type> PageAllocator::update_sync(const T& op)
{
  StatusOr<slot_offset_type> commit_slot = this->update(op);
  BATT_REQUIRE_OK(commit_slot);

  Status sync_status = this->sync(*commit_slot);
  BATT_REQUIRE_OK(sync_status);

  return commit_slot;
}

}  // namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_HPP
