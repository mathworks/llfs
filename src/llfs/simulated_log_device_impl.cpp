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

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDevice::Impl::Impl(StorageSimulation& simulation,
                                            u64 log_capacity) noexcept
    : simulation_{simulation}
    , capacity_{log_capacity}
    , writer_impl_{std::make_unique<WriterImpl>(*this)}
{
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
  auto locked_chunks = this->chunks_.lock();

  this->latest_recovery_step_.set_value(step);

  while (!locked_chunks->empty()) {
    // Check the latest commit chunk.
    //
    auto iter = std::prev(locked_chunks->end());

    // If the latest committed chunk is flushed, then we are done.
    //
    if (iter->second->flushed.get_value()) {
      break;
    }

    // Remove the unflushed chunk and keep going.
    //
    locked_chunks->erase(iter);
  }

  this->commit_pos_.set_value(this->flush_pos_.get_value());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::trim(slot_offset_type new_trim_pos) /*override*/
{
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
        BATT_CHECK_LT(iter->second->trim_size, iter->second->data.size())
            << "If all the data in a CommitChunk is trimmed, it should just be removed!";

        // Calculate where this chunk ends.
        //
        const slot_offset_type slot_upper_bound = iter->first + iter->second->data.size();

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
    return this->flush_pos_.get_value();
  };

  return std::make_unique<Impl::ReaderImpl>(
      *this, slot_lower_bound.or_else(get_default_slot_lower_bound), mode);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotRange SimulatedLogDevice::Impl::slot_range(LogReadMode mode) /*override*/
{
  auto locked_chunks = this->chunks_.lock();

  const slot_offset_type trim_pos = this->trim_pos_.get_value();

  auto iter = locked_chunks->end();
  while (iter != locked_chunks->begin()) {
    --iter;
    if (mode != LogReadMode::kDurable || iter->second->flushed.get_value()) {
      return SlotRange{trim_pos, iter->second->slot_offset + iter->second->data.size()};
    }
  }

  return SlotRange{trim_pos, trim_pos};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LogDevice::Writer& SimulatedLogDevice::Impl::writer() /*override*/
{
  return *this->writer_impl_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::close() /*override*/
{
  this->closed_.set_value(true);
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::sync(LogReadMode mode, SlotUpperBoundAt event) /*override*/
{
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
  while (iter != locked_chunks->end() && iter->second->flushed.get_value() == true) {
    // Advance flush_pos_ to include this (flushed) commit chunk.
    //
    clamp_min_slot(this->flush_pos_, iter->second->slot_upper_bound());

    // Next chunk, please.
    //
    ++iter;
  }
}

}  //namespace llfs
