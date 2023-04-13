//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_log_device_reader_impl.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDevice::Impl::ReaderImpl::ReaderImpl(Impl& impl,
                                                              slot_offset_type slot_offset,
                                                              LogReadMode mode) noexcept
    : impl_{impl}
    , slot_offset_{slot_offset}
    , mode_{mode}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool SimulatedLogDevice::Impl::ReaderImpl::is_closed() /*override*/
{
  return this->impl_.closed_.get_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer SimulatedLogDevice::Impl::ReaderImpl::data() /*override*/
{
  ConstBuffer buffer;

  {
    auto locked_chunks = this->impl_.chunks_.lock();

    this->impl_.log_event("reading slot ", this->slot_offset_);

    auto iter = locked_chunks->lower_bound(this->slot_offset_);
    if (iter != locked_chunks->end()) {
      std::shared_ptr<Impl::CommitChunk>& chunk = iter->second;
      if (this->mode_ != LogReadMode::kDurable || chunk->is_flushed()) {
        if (!slot_less_than(this->slot_offset_, chunk->slot_offset)) {
          const auto reader_offset_within_chunk = (this->slot_offset_ - chunk->slot_offset);
          if (reader_offset_within_chunk <= chunk->data.size()) {
            buffer = ConstBuffer{chunk->data.data() + reader_offset_within_chunk,
                                 chunk->data.size() - reader_offset_within_chunk};
            this->chunk_ = batt::make_copy(chunk);
          }
        }
      }
    }
  }

  this->data_size_ = buffer.size();

  this->impl_.log_event(" -- buffer.size=", buffer.size());

  return buffer;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDevice::Impl::ReaderImpl::consume(usize byte_count) /*override*/
{
  BATT_CHECK_LE(byte_count, this->data_size_);

  this->slot_offset_ += byte_count;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status SimulatedLogDevice::Impl::ReaderImpl::await(ReaderEvent event) /*override*/
{
  return batt::case_of(
      event,  //

      [&](const SlotUpperBoundAt& slot_upper_bound_at) -> Status {
        if (this->mode_ == LogReadMode::kDurable) {
          BATT_REQUIRE_OK(await_slot_offset(slot_upper_bound_at.offset, this->impl_.flush_pos_));
        } else {
          BATT_REQUIRE_OK(await_slot_offset(slot_upper_bound_at.offset, this->impl_.commit_pos_));
        }
        return batt::OkStatus();
      },

      [&](const BytesAvailable& bytes_available) -> Status {
        return this->await(SlotUpperBoundAt{.offset = this->slot_offset_ + bytes_available.size});
      });
}

}  //namespace llfs
