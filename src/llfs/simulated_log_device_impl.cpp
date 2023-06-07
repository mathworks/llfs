//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_log_device_impl.hpp>
//

#include <llfs/simulated_log_device_reader_impl.hpp>
#include <llfs/simulated_log_device_writer_impl.hpp>
#include <llfs/system_config.hpp>

#include <algorithm>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDevice::Impl::Impl(StorageSimulation& simulation, const std::string& name,
                                            u64 log_capacity) noexcept
    : simulation_{simulation}
    , name_{name}
    , capacity_{log_capacity}
    , ring_buffer_{RingBuffer::TempFile{
          .byte_size = this->capacity_,
      }}
    , writer_impl_{std::make_unique<WriterImpl>(*this)}
{
  BATT_CHECK_EQ(round_up_to_page_size_multiple(this->capacity_), this->capacity_)
      << "Capacity must be a multiple of the memory page size";

  u64* const buffer_begin = static_cast<u64*>(this->ring_buffer_.get_mut(0).data());
  u64* const buffer_end = buffer_begin + (this->capacity_ / sizeof(u64));

  std::fill(buffer_begin, buffer_end, u64{0x0bad1bad2bad3badull});
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SimulatedLogDevice::Impl::~Impl() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDevice::Impl::crash_and_recover(u64 step) /*override*/
{
  this->log_event("entered crash_and_recover(step=", step, ")");
  auto on_scope_exit = batt::finally([&] {
    this->log_event("leaving crash_and_recover(step=", step, ")");
  });

  auto locked_chunks = this->chunks_.lock();

  this->latest_recovery_step_.set_value(step);

  while (!locked_chunks->empty()) {
    // Check the latest commit chunk.
    //
    auto iter = std::prev(locked_chunks->end());

    // If the latest committed chunk is flushed, then we are done.
    //
    const batt::Optional<i32> dropped =
        iter->second->state.modify_if([](i32 old_value) -> batt::Optional<i32> {
          if (old_value == CommitChunk::kCommittedState) {
            return CommitChunk::kDroppedState;
          }
          return batt::None;
        });

    if (!dropped) {
      break;
    }
    this->log_event("(crash_and_recover) dropped slot_range ", iter->second->slot_range());

    // Remove the unflushed chunk and keep going.
    //
    locked_chunks->erase(iter);
  }

  this->commit_pos_.set_value(this->flush_pos_.get_value());

  // IMPORTANT: these must be reset() after we update `latest_recover_step_`!
  //
  this->commit_pos_.reset();
  this->flush_pos_.reset();

  this->closed_.set_value(false);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::trim(u64 device_create_step,
                                      slot_offset_type new_trim_pos) /*override*/
{
  this->log_event("trim(", new_trim_pos, "), device_create_step=", device_create_step);

  if (this->latest_recovery_step_.get_value() > device_create_step) {
    this->log_event(" -- ignoring (obsolete LogDevice)");
    return batt::StatusCode::kClosed;
  }

  // Update the volatile trim pos.  If no change (clamp_min_slot returns delta == 0), return.
  //
  if (clamp_min_slot(this->trim_pos_, new_trim_pos) != 0) {
    // Do the actual removal of chunks in a deferred action to simulate the I/O necesssary to harden
    // the new trim_pos.
    //
    this->simulation_.post([this, current_step = this->latest_recovery_step_.get_value(),
                            new_trim_pos] {
      auto locked_chunks = this->chunks_.lock();

      // After acquiring the lock, we make sure that this action wasn't scheduled before the most
      // recent simulated crash/recovery.
      //
      if (current_step < this->latest_recovery_step_.get_value()) {
        return;
      }

      while (!locked_chunks->empty()) {
        // Look at the first (oldest) chunk.
        //
        auto iter = locked_chunks->begin();

        // Sanity checks.
        //
        BATT_CHECK_EQ(iter->first, iter->second->slot_offset);
        BATT_CHECK_LT(iter->second->trim_size, iter->second->data_view.size())
            << "If all the data in a CommitChunk is trimmed, it should just be removed!";

        // Calculate where this chunk ends.
        //
        const slot_offset_type slot_upper_bound = iter->first + iter->second->data_view.size();

        // If the new trim pos is at least the slot_upper_bound, then drop the chunk entirely and
        // keep going.
        //
        if (slot_at_least(new_trim_pos, slot_upper_bound)) {
          locked_chunks->erase(iter);
          continue;
        }

        // This is the last chunk to trim; adjust the trim size and exit.
        //
        if (slot_greater_than(new_trim_pos, iter->first)) {
          iter->second->trim_size = std::max(iter->second->trim_size, new_trim_pos - iter->first);
        }
        break;
      }
    });
  }

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<LogDevice::Reader> SimulatedLogDevice::Impl::new_reader(
    Optional<slot_offset_type> slot_lower_bound, LogReadMode mode) /*override*/
{
  auto get_default_slot_lower_bound = [this] {
    return this->trim_pos_.get_value();
  };

  return std::make_unique<Impl::ReaderImpl>(
      *this, slot_lower_bound.or_else(get_default_slot_lower_bound), mode);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotRange SimulatedLogDevice::Impl::slot_range(LogReadMode mode) /*override*/
{
  return SlotRange{
      .lower_bound = this->trim_pos_.get_value(),
      .upper_bound = this->get_slot_upper_bound(mode),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LogDevice::Writer& SimulatedLogDevice::Impl::writer() /*override*/
{
  return *this->writer_impl_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::close(u64 device_create_step) /*override*/
{
  this->log_event("close()");
  if (this->latest_recovery_step_.get_value() > device_create_step) {
    this->log_event(" -- ignoring (obsolete LogDevice)");
    return batt::OkStatus();
  }
  this->closed_.set_value(true);
  this->flush_pos_.close();
  this->commit_pos_.close();

  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::sync(u64 device_create_step, LogReadMode mode,
                                      SlotUpperBoundAt event) /*override*/
{
  BATT_DEBUG_INFO("SimulatedLogDevice::sync(mode="
                  << mode << ", event=" << event << ") flush_pos=" << this->flush_pos_.get_value()
                  << ", commit_pos=" << this->commit_pos_.get_value());

  if (this->latest_recovery_step_.get_value() > device_create_step) {
    return batt::StatusCode::kClosed;
  }

  if (mode == LogReadMode::kDurable) {
    BATT_REQUIRE_OK(await_slot_offset(event.offset, this->flush_pos_));
  } else {
    BATT_REQUIRE_OK(await_slot_offset(event.offset, this->commit_pos_));
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDevice::Impl::update_flush_pos()
{
  auto locked_chunks = this->chunks_.lock();

  // Start at the chunk closest to the current flush pos.
  //
  auto iter = locked_chunks->lower_bound(this->flush_pos_.get_value());

  // Keep iterating through chunks until we find one that hasn't been flushed...
  //
  while (iter != locked_chunks->end() && iter->second->is_flushed()) {
    // Advance flush_pos_ to include this (flushed) commit chunk.
    //
    clamp_min_slot(this->flush_pos_, iter->second->slot_upper_bound());

    // Next chunk, please.
    //
    ++iter;
  }
}

}  //namespace llfs
