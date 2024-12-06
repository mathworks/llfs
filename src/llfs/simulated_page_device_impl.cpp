//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_page_device_impl.hpp>
//

#include <llfs/storage_simulation.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedPageDevice::Impl::Impl(StorageSimulation& simulation, const std::string& name,
                                             PageSize page_size, PageCount page_count,
                                             page_device_id_int device_id) noexcept
    : simulation_{simulation}
    , name_{name}
    , page_size_{page_size}
    , page_count_{page_count}
    , device_id_{device_id}
{
  BATT_CHECK_EQ(this->blocks_per_page_ * kDataBlockSize, this->page_size_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::Impl::crash_and_recover(u64 step) /*override*/
{
  this->log_event("entered crash_and_recover(step=", step, ")");
  auto on_scope_exit = batt::finally([&] {
    this->log_event("leaving crash_and_recover(step=", step, ")");
  });

  auto locked_blocks = this->blocks_.lock();

  this->latest_recovery_step_.set_value(step);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<bool> SimulatedPageDevice::Impl::has_data_for_page_id(PageId page_id) const noexcept
{
  const i64 physical_page = this->get_physical_page(page_id);
  i64 count = 0;
  {
    auto locked_blocks = this->blocks_.lock();
    this->for_each_page_block(physical_page, [&](i64 /*block_0*/, i64 block_i) {
      if (locked_blocks->count(block_i)) {
        count += 1;
      }
    });
  }
  if (count == 0) {
    return {false};
  }
  if (count == this->blocks_per_page_) {
    return {true};
  }
  return {batt::StatusCode::kUnknown};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> SimulatedPageDevice::Impl::prepare(u32 device_create_step,
                                                                         PageId page_id)
{
  this->log_event("prepare(", page_id, "), device_create_step=", device_create_step);

  if (this->latest_recovery_step_.get_value() > device_create_step) {
    this->log_event(" -- ignoring (obsolete PageDevice)");
    return {batt::StatusCode::kClosed};
  }

  BATT_CHECK_EQ(this->device_id_, this->page_id_factory_.get_device_id(page_id));

  const i64 physical_page = this->get_physical_page(page_id);
  if (physical_page < 0 || physical_page >= BATT_CHECKED_CAST(i64, this->page_count_.value())) {
    return {batt::StatusCode::kInvalidArgument};
  }

  const page_device_id_int device_id = this->page_id_factory_.get_device_id(page_id);
  if (device_id != this->device_id_) {
    return {batt::StatusCode::kInvalidArgument};
  }

  return PageBuffer::allocate(this->page_size_, page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::Impl::write(u32 device_create_step,
                                      std::shared_ptr<const PageBuffer>&& page_buffer,
                                      PageDevice::WriteHandler&& handler)
{
  this->log_event("write(", page_buffer->page_id(), "[size=", page_buffer->size(), "])");

  if (this->latest_recovery_step_.get_value() > device_create_step) {
    this->log_event(" -- ignoring (obsolete PageDevice)");
    std::move(handler)({batt::StatusCode::kClosed});
    return;
  }

  const PageId page_id = page_buffer->page_id();
  BATT_CHECK_EQ(this->device_id_, this->page_id_factory_.get_device_id(page_id));

  const i64 physical_page = this->get_physical_page(page_id);
  if (!this->validate_physical_page_async(physical_page, handler)) {
    return;
  }

  auto op = batt::make_shared<MultiBlockOp>(*this);
  {
    const u64 current_step = this->latest_recovery_step_.get_value();

    this->for_each_page_block(physical_page, [&](i64 block_0, i64 block_i) {
      this->simulation_.post(
          [this, block_0, block_i, current_step, op, page_buffer = batt::make_copy(page_buffer)] {
            this->log_event("writing block ", block_i, " (block_0=", block_0, ")");

            {
              //----- --- -- -  -  -   -
              auto locked_blocks = this->blocks_.lock();

              // If `latest_recovery_step_` has changed, then we are on the other side of a
              // simulated crash/recovery; do nothing.
              //
              const u64 latest_recovery_step = this->latest_recovery_step_.get_value();
              if (current_step != latest_recovery_step) {
                this->log_event(" -- operation is obsolete!  skipping (current_step=", current_step,
                                ", latest_recovery_step=", latest_recovery_step);
                return;
              }

              // If the simulation decides that a failure should be injected at this point, then
              // generate one and notify the op.
              //
              if (this->simulation_.inject_failure()) {
                this->log_event(" -- injecting failure");
                op->set_block_result(block_i - block_0, batt::Status{batt::StatusCode::kInternal});
                return;
              }
              this->log_event(" -- (block write will succeed)");

              // No failure injected; grab the slice of the PageBuffer corresponding to this block.
              //
              ConstBuffer source = batt::slice_buffer(page_buffer->const_buffer(),
                                                      batt::Interval<usize>{
                                                          (block_i - block_0) * kDataBlockSize,
                                                          (block_i - block_0 + 1) * kDataBlockSize,
                                                      });

              // Sanity checks (make sure sizes are all correct).
              //
              BATT_CHECK_EQ(source.size(), kDataBlockSize);
              static_assert(kDataBlockSize == sizeof(DataBlock));

              // Allocate a block buffer and copy our slice of the PageBuffer to it.
              {
                auto block = std::make_unique<DataBlock>();
                std::memcpy(block.get(), source.data(), source.size());
                locked_blocks->emplace(block_i, std::move(block));
              }
            }

            // Notify the op that this block has completed.
            //
            op->set_block_result(block_i - block_0, batt::OkStatus());
          });
    });
  }
  op->on_completion(
      [this, page_id, handler = std::move(handler)](const batt::Status& status) mutable {
        this->log_event("write(", page_id, ") completed with status ", status);
        std::move(handler)(status);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::Impl::read(u32 device_create_step, PageId page_id,
                                     PageDevice::ReadHandler&& handler)
{
  this->log_event("read(", page_id, ")");

  if (this->latest_recovery_step_.get_value() > device_create_step) {
    this->log_event(" -- ignoring (obsolete PageDevice)");
    std::move(handler)({batt::StatusCode::kClosed});
    return;
  }

  BATT_CHECK_EQ(this->device_id_, this->page_id_factory_.get_device_id(page_id));

  const i64 physical_page = this->get_physical_page(page_id);
  if (!this->validate_physical_page_async(physical_page, handler)) {
    return;
  }

  std::shared_ptr<PageBuffer> page_buffer = PageBuffer::allocate(this->page_size_, page_id);
  auto op = batt::make_shared<MultiBlockOp>(*this);

  // We do a piecewise read of each block at a different step so we can simulate the effect
  // of data races at the block device level.
  //
  this->for_each_page_block(physical_page, [&](i64 block_0, i64 block_i) {
    this->simulation_.post([this, block_0, block_i, page_buffer, op] {
      //----- --- -- -  -  -   -
      auto locked_blocks = this->blocks_.lock();

      // Find the slice of the page buffer corresponding to this block.
      //
      MutableBuffer target = batt::slice_buffer(page_buffer->mutable_buffer(),
                                                batt::Interval<usize>{
                                                    (block_i - block_0) * kDataBlockSize,
                                                    (block_i - block_0 + 1) * kDataBlockSize,
                                                });

      // Copy the data or all zeros (if this block was trimmed or never initialized).
      //
      auto iter = locked_blocks->find(block_i);
      if (iter == locked_blocks->end()) {
        this->log_event("read block ", block_i, " -- no previous writes (returning zeros)");
        std::memset(target.data(), 0, target.size());
      } else {
        this->log_event("read block ", block_i, " -- previous write found");
        std::memcpy(target.data(), iter->second.get(), target.size());
      }

      // Notify the op that this block has completed.
      //
      op->set_block_result(block_i - block_0, batt::OkStatus());
    });
  });

  // Since read has no durable side-effects, there's no reason to simulate partial
  // success/failure. We succeed entirely or not at all (hence the single call to
  // `inject_failure()` instead of one for each block).
  //
  op->on_completion([this, page_id,                                     //
                     should_fail = this->simulation_.inject_failure(),  //
                     page_buffer = std::move(page_buffer),              //
                     handler = std::move(handler)](                     //
                        const batt::Status& status) mutable {
    if (!status.ok()) {
      this->log_event("read(", page_id, ") failed with status ", status);
      std::move(handler)({status});

    } else if (should_fail) {
      this->log_event("read(", page_id, ") -- failure injected (kUnavailable)");
      std::move(handler)({batt::StatusCode::kUnavailable});

    } else {
      this->log_event("read(", page_id, ") ok");
      std::move(handler)({std::move(page_buffer)});
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::Impl::drop(u32 device_create_step, PageId page_id,
                                     PageDevice::WriteHandler&& handler)
{
  this->log_event("drop(", page_id, ")");

  if (this->latest_recovery_step_.get_value() > device_create_step) {
    this->log_event(" -- ignoring (obsolete PageDevice)");
    std::move(handler)({batt::StatusCode::kClosed});
    return;
  }

  BATT_CHECK_EQ(this->device_id_, this->page_id_factory_.get_device_id(page_id));

  const i64 physical_page = this->get_physical_page(page_id);
  if (!this->validate_physical_page_async(physical_page, handler)) {
    return;
  }

  auto op = batt::make_shared<MultiBlockOp>(*this);
  {
    const u64 current_step = this->latest_recovery_step_.get_value();

    this->for_each_page_block(physical_page, [&](i64 block_0, i64 block_i) {
      this->simulation_.post([this, block_0, block_i, current_step, op] {
        auto locked_blocks = this->blocks_.lock();

        this->log_event("dropping block ", block_i, " (block_0=", block_0, ")");

        // If `latest_recovery_step_` has changed, then we are on the other side of a simulated
        // crash/recovery; do nothing.
        //
        const u64 latest_recovery_step = this->latest_recovery_step_.get_value();
        if (current_step != latest_recovery_step) {
          this->log_event(" -- operation is obsolete!  skipping (current_step=", current_step,
                          ", latest_recovery_step=", latest_recovery_step);
          return;
        }

        // If the simulation decides that a failure should be injected at this point, then
        // generate one and notify the op.
        //
        if (this->simulation_.inject_failure()) {
          this->log_event(" -- injecting failure (kInternal)");
          op->set_block_result(block_i - block_0, batt::Status{batt::StatusCode::kInternal});
          return;
        }

        // No failure injected.
        // Note that we are not not erasing the data block as we want to mimic the actual
        // io_ring Page Device behavior.
        //
        op->set_block_result(block_i - block_0, batt::OkStatus());
      });
    });
  }
  op->on_completion(
      [this, page_id, handler = std::move(handler)](const batt::Status& status) mutable {
        this->log_event("drop(", page_id, ") completed with status ", status);
        std::move(handler)(status);
      });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
i64 SimulatedPageDevice::Impl::get_physical_page(PageId page_id) const noexcept
{
  return this->page_id_factory_.get_physical_page(page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Handler>
bool SimulatedPageDevice::Impl::validate_physical_page_async(i64 physical_page, Handler&& handler)
{
  if (physical_page < 0 || physical_page >= BATT_CHECKED_CAST(i64, this->page_count_.value())) {
    this->log_event(" -- physical_page=", physical_page,
                    " not valid for this device! (kInvalidArgument)");
    std::move(handler)(batt::Status{batt::StatusCode::kInvalidArgument});
    return false;
  }
  return true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename Fn>
void SimulatedPageDevice::Impl::for_each_page_block(i64 physical_page, Fn&& fn) const noexcept
{
  const i64 first_block = physical_page * this->blocks_per_page_;
  const i64 last_block = first_block + this->blocks_per_page_;

  for (i64 block_i = first_block; block_i < last_block; ++block_i) {
    fn(first_block, block_i);
  }
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// struct SimulatedPageDevice::Impl::MultiBlockOp

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedPageDevice::Impl::MultiBlockOp::MultiBlockOp(Impl& impl) noexcept
    : impl_{impl}
    , pending_blocks_{this->impl_.blocks_per_page_}
    , block_status_(this->impl_.blocks_per_page_, batt::StatusCode::kUnknown)
{
  BATT_CHECK_EQ(BATT_CHECKED_CAST(usize, this->pending_blocks_.get_value()),
                this->block_status_.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::Impl::MultiBlockOp::set_block_result(i64 relative_block_i,
                                                               const batt::Status& status)
{
  BATT_CHECK_LT(BATT_CHECKED_CAST(usize, relative_block_i), this->block_status_.size())
      << "relative_block_i is out-of-range; forgot to subtract block_0?";
  BATT_CHECK_NE(status, batt::StatusCode::kUnknown);
  BATT_CHECK_EQ(this->block_status_[relative_block_i], batt::StatusCode::kUnknown);

  const i64 observed_pending = this->pending_blocks_.get_value();

  this->impl_.log_event("set_block_result(", relative_block_i, ", ", status,
                        "), pending_blocks: ", observed_pending, "->", observed_pending - 1);

  this->block_status_[relative_block_i] = status;
  this->pending_blocks_.fetch_sub(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedPageDevice::Impl::MultiBlockOp::on_completion(PageDevice::WriteHandler&& handler)
{
  auto shared_this = batt::shared_ptr_from(this);

  const i64 observed_pending = this->pending_blocks_.get_value();
  if (observed_pending == 0) {
    batt::Status combined_status = batt::OkStatus();
    for (const auto& block_r : shared_this->block_status_) {
      combined_status.Update(batt::to_status(block_r));
    }
    std::move(handler)(combined_status);
    return;
  }

  // There are pending blocks.  Receive notification when `this->pending_blocks_` changes from the
  // value observed above.
  //
  this->pending_blocks_.async_wait(
      observed_pending,
      batt::bind_handler(std::move(handler),
                         [shared_this = std::move(shared_this)](PageDevice::WriteHandler&& handler,
                                                                StatusOr<i64> updated) {
                           if (!updated.ok()) {
                             std::move(handler)(batt::StatusCode::kCancelled);
                           } else {
                             shared_this->on_completion(std::move(handler));
                           }
                         }));
}

}  //namespace llfs
