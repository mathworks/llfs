#include <llfs/page_allocator.hpp>
//

#include <llfs/data_packer.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/metrics.hpp>
#include <llfs/slot_reader.hpp>

#include <batteries/async/runtime.hpp>

#include <glog/logging.h>

#include <boost/range/irange.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace llfs {

u64 PageAllocator::calculate_log_size(u64 physical_page_count, u64 max_attachments)
{
  static const PackedPageRefCount packed_ref_count{
      .page_id = 0,
      .ref_count = 0,
  };
  static const PackedPageAllocatorAttach packed_attachment{.user_slot = {
                                                               .user_id = {},
                                                               .slot_offset = 0,
                                                           }};

  const u64 attachment_checkpoints = packed_sizeof_slot(packed_attachment) * max_attachments;

  const u64 refcount_checkpoints =
      (packed_sizeof_slot(packed_attachment) + packed_sizeof_slot(packed_ref_count)) *
      physical_page_count;

  const u64 max_checkpoint_size = attachment_checkpoints + refcount_checkpoints;

  const u64 max_transaction_size = packed_sizeof_page_allocator_txn(physical_page_count);

  const u64 rough_size = (max_checkpoint_size + max_transaction_size + kCheckpointGrantSize) * 4 +
                         kCheckpointGrantSize - 1;

  return rough_size - rough_size % kCheckpointGrantSize;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<PageAllocator>> PageAllocator::recover(
    batt::TaskScheduler& scheduler, std::string_view name, const PageIdFactory& page_ids,
    LogDeviceFactory& log_device_factory)
{
  initialize_status_codes();

  // Create a State object to collect recovered events from the log.
  //
  auto recovered_state = std::make_unique<PageAllocator::State>(page_ids);

  PageAllocator::Metrics metrics;

  // For each recovered event in the log, update the state machine.
  //
  auto process_recovered_event = [&recovered_state, &metrics](const SlotParse& slot,
                                                              const auto& event_payload) -> Status {
    if (!PageAllocatorState::is_valid(recovered_state->propose(event_payload))) {
      return StatusCode::kInvalidPageAllocatorProposal;
    }

    recovered_state->learn(slot.offset.lower_bound, event_payload, metrics);
    return OkStatus();
  };

  // Read the log, scanning its contents.
  //
  StatusOr<std::unique_ptr<LogDevice>> recovered_log = log_device_factory.open_log_device(
      [&process_recovered_event](LogDevice::Reader& log_reader) -> StatusOr<slot_offset_type> {
        TypedSlotReader<PackedPageAllocatorEvent> slot_reader{log_reader};

        BATT_ASSIGN_OK_RESULT(usize slots_recovered, slot_reader.run(batt::WaitForResource::kFalse,
                                                                     process_recovered_event));

        VLOG(1) << "PageAllocator recovered log: " << BATT_INSPECT(slots_recovered);

        return log_reader.slot_offset();
      });

  BATT_REQUIRE_OK(recovered_log);

  // Now we can create the PageAllocator.
  //
  return std::unique_ptr<PageAllocator>{
      new PageAllocator{scheduler, name, std::move(*recovered_log), std::move(recovered_state)}};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageAllocator::PageAllocator(batt::TaskScheduler& scheduler, std::string_view name,
                             std::unique_ptr<LogDevice> log_device,
                             std::unique_ptr<PageAllocator::State> recovered_state) noexcept
    : name_{name}
    , log_device_{std::move(log_device)}
    , state_{std::move(recovered_state)}
    , checkpoint_task_{scheduler.schedule_task(),
                       [this] {
                         this->checkpoint_task_main();
                       },
                       "[PageAllocator::checkpoint_task]"}
{
  VLOG(1) << "PageAllocator() (this=" << (void*)this << ")";

  const auto metric_name = [this](std::string_view property) {
    return batt::to_string("PageAllocator_", this->name_, "_", property);
  };

#define ADD_METRIC_(n) global_metric_registry().add(metric_name(#n), this->metrics_.n)

  ADD_METRIC_(pages_allocated);
  ADD_METRIC_(pages_freed);

#undef ADD_METRIC_
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

PageAllocator::~PageAllocator() noexcept
{
  VLOG(1) << "~PageAllocator() (this=" << (void*)this << ")";

  this->halt();
  this->join();

  global_metric_registry()  //
      .remove(this->metrics_.pages_allocated)
      .remove(this->metrics_.pages_freed);
}

void PageAllocator::halt() noexcept
{
  VLOG(1) << "PageAllocator::halt()";
  VLOG(2) << boost::stacktrace::stacktrace{};

  this->stop_requested_.set_value(true);

  this->slot_writer_.halt();
  this->log_device_->close().IgnoreError();
  this->state_->halt();
  this->checkpoint_grant_.revoke();
}

void PageAllocator::join() noexcept
{
  this->checkpoint_task_.join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<PageId> PageAllocator::allocate_page(batt::WaitForResource wait_for_resource)
{
  for (;;) {
    {
      auto locked = this->state_.lock();
      Optional<PageId> page_id = locked->get()->allocate_page();
      if (page_id) {
        this->metrics_.pages_allocated.fetch_add(1);
        return *page_id;
      }
      LOG_FIRST_N(INFO, 1) << "Unable to allocate page (pool is empty)";
      if (wait_for_resource == batt::WaitForResource::kFalse) {
        return Status{
            batt::StatusCode::kUnavailable};  // TODO [tastolfi 2021-10-20] "the pool is empty"
      }
    }
    BATT_DEBUG_INFO("[PageAllocator::allocate_page] waiting for free page");
    auto wait_status = this->state_->await_free_page();
    BATT_REQUIRE_OK(wait_status);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::deallocate_page(PageId page_id)
{
  VLOG(1) << "page deallocated: " << page_id;
  this->state_.lock()->get()->deallocate_page(page_id);
  this->metrics_.pages_freed.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocator::recover_page(PageId page_id)
{
  VLOG(1) << "removing page from free pool for recovery: " << page_id;
  return this->state_.lock()->get()->recover_page(page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> PageAllocator::attach_user(const boost::uuids::uuid& user_id,
                                                      slot_offset_type user_slot)
{
  return this->update_sync(PackedPageAllocatorAttach{
      .user_slot =
          PackedPageUserSlot{
              .user_id = user_id,
              .slot_offset = user_slot,
          },
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> PageAllocator::detach_user(const boost::uuids::uuid& user_id,
                                                      slot_offset_type user_slot)
{
  return this->update_sync(PackedPageAllocatorDetach{
      .user_slot =
          PackedPageUserSlot{
              .user_id = user_id,
              .slot_offset = user_slot,
          },
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::pair<i32, slot_offset_type> PageAllocator::get_ref_count(PageId id)
{
  return this->state_->get_ref_count(id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
BoxedSeq<PageRefCount> PageAllocator::page_ref_counts()
{
  auto* state = &(this->state_.no_lock());

  return as_seq(boost::irange<std::size_t>(0, state->page_device_capacity()))  //
         | seq::map([state](page_id_int id_val) {
             return state->get_ref_count_obj(PageId{id_val});
           })  //
         | seq::boxed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void PageAllocator::checkpoint_task_main()
{
  Status final_status = [this]() -> Status {
    usize count = 0;
    for (;;) {
      BATT_DEBUG_INFO("Wait for checkpoint_grant; log size="
                      << this->slot_writer_.log_size() << "/" << this->slot_writer_.log_capacity()
                      << " checkpoint_grant=" << this->checkpoint_grant_
                      << " pool=" << this->slot_writer_.pool_size());

      // Reserve some space in the log to write the next slice of checkpoint data.
      //
      StatusOr<batt::Grant> slice_grant =
          this->checkpoint_grant_.spend(kCheckpointGrantSize, batt::WaitForResource::kTrue);

      BATT_REQUIRE_OK(slice_grant)
          << "Could not spend checkpoint grant to make a new slice;"
          << BATT_INSPECT(kCheckpointGrantSize) << BATT_INSPECT(this->checkpoint_grant_);

      // At the end of each loop iteration, recycle whatever part of the slice grant we didn't
      // use.
      //
      auto grant_recycler = batt::finally([&] {
        this->checkpoint_grant_.subsume(std::move(*slice_grant));
      });

      slot_offset_type new_trim_pos = 0;
      const auto slice_grant_before = slice_grant->size();

      slot_offset_type prior_learned_slot = this->state_->learned_upper_bound();
      {
        BATT_DEBUG_INFO("lock state");
        auto locked = this->state_.lock();

        // Tell the locked state machine to write out a batch of checkpoint slice data.
        //
        auto write_checkpoint_status =
            locked->get()->write_checkpoint_slice(this->slot_writer_, *slice_grant);
        BATT_REQUIRE_OK(write_checkpoint_status);

        new_trim_pos = *write_checkpoint_status;
      }

      // If no checkpoint slice was written, then wait for more data and try again.
      //
      if (slice_grant_before == slice_grant->size()) {
        BATT_DEBUG_INFO("no checkpoint written; waiting for more data; "
                        << BATT_INSPECT(slice_grant->size()) << BATT_INSPECT(prior_learned_slot));

        auto slot_status = this->state_->await_learned_slot(prior_learned_slot + 1);
        BATT_REQUIRE_OK(slot_status);
        continue;
      }

      BATT_DEBUG_INFO("awaiting consumed upper_bound: " << new_trim_pos);

      const slot_offset_type old_trim_pos =
          this->log_device_->slot_range(LogReadMode::kSpeculative).lower_bound;

      StatusOr<slot_offset_type> trim_status = this->slot_writer_.trim(new_trim_pos);
      BATT_REQUIRE_OK(trim_status);

      count += 1;

      VLOG(2) << "PageAllocator checkpoint written (count=" << count
              << ", trim_size=" << (new_trim_pos - old_trim_pos)
              << ", slice_size=" << (slice_grant_before - slice_grant->size())
              << "); trim_pos: " << old_trim_pos << " -> " << new_trim_pos
              << " log_full=" << this->slot_writer_.log_size() << "/"
              << this->slot_writer_.log_capacity() << " in_use=" << this->slot_writer_.in_use_size()
              << " avail=" << this->slot_writer_.pool_size();
    }
  }();

  if (this->stop_requested_.get_value()) {
    VLOG(1) << "[PageAllocator::checkpoint_task] exited with status=" << final_status;
  } else {
    if (!suppress_log_output_for_test()) {
      LOG(ERROR) << "[PageAllocator::checkpoint_task] exited unexpectedly with status="
                 << final_status;
    }
    this->checkpoint_grant_.revoke();
    this->slot_writer_.halt();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status PageAllocator::sync(slot_offset_type min_slot)
{
  BATT_DEBUG_INFO("update_sync: awaiting flushed upper_bound: " << min_slot);

  Status flush_status = this->log_device_->sync(LogReadMode::kDurable, SlotUpperBoundAt{
                                                                           .offset = min_slot,
                                                                       });

  BATT_REQUIRE_OK(flush_status);

  // Push the durable upper bound up, but don't wait for the learned upper bound to catch up until
  // we try to read (if a tree falls in the forest...)
  //
  clamp_min_slot(this->durable_upper_bound_, min_slot);

  return OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::vector<PageAllocatorAttachmentStatus> PageAllocator::get_all_clients_attachment_status() const
{
  auto locked = this->state_.lock();
  return locked->get()->get_all_clients_attachment_status();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<PageAllocatorAttachmentStatus> PageAllocator::get_client_attachment_status(
    const boost::uuids::uuid& uuid) const
{
  auto locked = this->state_.lock();
  return locked->get()->get_client_attachment_status(uuid);
}

}  // namespace llfs
