//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_log_device_writer_impl.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDevice::Impl::WriterImpl::WriterImpl(Impl& impl) noexcept : impl_{impl}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u64 SimulatedLogDevice::Impl::WriterImpl::space() const /*override*/
{
  return this->impl_.space();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type SimulatedLogDevice::Impl::WriterImpl::slot_offset() /*override*/
{
  return this->impl_.commit_pos_.get_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<MutableBuffer> SimulatedLogDevice::Impl::WriterImpl::prepare(usize byte_count,
                                                                      usize head_room) /*override*/
{
  this->impl_.log_event("prepare(", byte_count, ", extra=", head_room,
                        "); closed=", this->impl_.closed_.get_value());

  if (this->impl_.closed_.get_value()) {
    return {batt::StatusCode::kClosed};
  }

  BATT_REQUIRE_OK(this->await(BytesAvailable{
      .size = byte_count + head_room,
  }));

  BATT_CHECK_EQ(this->prepared_chunk_, nullptr);

  this->prepared_chunk_ = std::make_shared<Impl::CommitChunk>(
      this->impl_, /*offset=*/this->slot_offset(), /*size=*/byte_count);

  return MutableBuffer{this->prepared_chunk_->data.data(), this->prepared_chunk_->data.size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> SimulatedLogDevice::Impl::WriterImpl::commit(
    usize byte_count) /*override*/
{
  // Sanity check: prepare must be called for every commit.
  //
  BATT_CHECK_NOT_NULLPTR(this->prepared_chunk_);

  this->impl_.log_event("commit(", byte_count,
                        "); slot_range=", this->prepared_chunk_->slot_range(),
                        " closed=", this->impl_.closed_.get_value());

  if (this->impl_.closed_.get_value()) {
    return {batt::StatusCode::kClosed};
  }

  // Clear `prepared_chunk_` by moving to a local variable.
  //
  auto committed_chunk = std::move(this->prepared_chunk_);

  // Move the state from prepared to committed, run sanity check.
  //
  const i32 old_state = committed_chunk->state.set_value(CommitChunk::kCommittedState);
  BATT_CHECK_EQ(old_state, CommitChunk::kPreparedState);

  // Sanity check: commit may not be passed a larger size than was passed to prepare.
  //
  BATT_CHECK_LE(byte_count, committed_chunk->data.size());

  // Truncate the data buffer to only include the committed amount.
  //
  committed_chunk->data.resize(byte_count);

  // Put the chunk in the simulated device's chunk map, but don't update the flushed flag (or
  // flush_pos_) yet.
  //
  const slot_offset_type chunk_slot_offset = committed_chunk->slot_offset;
  {
    auto locked_chunks = this->impl_.chunks_.lock();
    locked_chunks->emplace(committed_chunk->slot_offset, batt::make_copy(committed_chunk));
  }

  auto& sim = this->impl_.simulation_;

  // Should the flush fail?
  //
  if (sim.inject_failure()) {
    sim.post([this, committed_chunk = std::move(committed_chunk)] {
      // The flush_pos will never be updated again, so wake up any tasks who are waiting on it.
      //
      this->impl_.flush_pos_.close();

      this->impl_.log_event("slot_range ", committed_chunk->slot_range(),
                            " flush failed (error injected)");
    });

  } else {
    // We are not injecting a failure; defer completion of the flush.
    //
    sim.post([this, committed_chunk = std::move(committed_chunk)] {
      if (this->impl_.closed_.get_value()) {
        this->impl_.log_event("slot_range ", committed_chunk->slot_range(),
                              " not flushed (device closed)");
        return;
      }

      const batt::Optional<i32> flushed =
          committed_chunk->state.modify_if([](i32 old_value) -> batt::Optional<i32> {
            if (old_value == CommitChunk::kCommittedState) {
              return CommitChunk::kFlushedState;
            }
            return batt::None;
          });

      if (flushed) {
        this->impl_.log_event("slot_range ", committed_chunk->slot_range(), " flushed");
        this->impl_.update_flush_pos();
      }
    });
  }

  const slot_offset_type old_commit_pos = this->impl_.commit_pos_.fetch_add(byte_count);
  const slot_offset_type new_commit_pos = old_commit_pos + byte_count;

  BATT_CHECK_EQ(old_commit_pos, chunk_slot_offset);

  return {new_commit_pos};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::WriterImpl::await(LogDevice::WriterEvent event) /*override*/
{
  return batt::case_of(
      event,

      [&](const SlotLowerBoundAt& slot_lower_bound_at) -> batt::Status {
        BATT_REQUIRE_OK(await_slot_offset(slot_lower_bound_at.offset, this->impl_.trim_pos_));
        return batt::OkStatus();
      },

      [&](const BytesAvailable& bytes_available) -> batt::Status {
        const slot_offset_type observed_trim = this->impl_.trim_pos_.get_value();
        const slot_offset_type observed_commit = this->impl_.commit_pos_.get_value();

        BATT_CHECK_LE(observed_trim, observed_commit);

        const u64 observed_size = observed_commit - observed_trim;
        const u64 observed_space = this->impl_.capacity() - observed_size;
        if (observed_space >= bytes_available.size) {
          return batt::OkStatus();
        }

        const u64 need_to_trim_size = bytes_available.size - observed_space;

        return this->await(SlotLowerBoundAt{
            .offset = observed_trim + need_to_trim_size,
        });
      });
}

}  //namespace llfs
