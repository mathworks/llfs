//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_writer.hpp>
//

namespace llfs {

SlotWriter::SlotWriter(LogDevice& log_device) noexcept : log_device_{log_device}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::Grant> SlotWriter::reserve(u64 size, batt::WaitForResource wait_for_resource)
{
  return this->pool_.issue_grant(size, wait_for_resource);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> SlotWriter::trim(slot_offset_type slot_lower_bound)
{
  slot_offset_type delta = clamp_min_slot(this->trim_lower_bound_, slot_lower_bound);

  Status status = this->log_device_.trim(slot_lower_bound);
  BATT_REQUIRE_OK(status);

  // Return the trimmed bytes to the pool.
  //
  this->in_use_.spend(delta).IgnoreError();

  return delta;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<batt::Grant> SlotWriter::trim_and_reserve(slot_offset_type slot_lower_bound)
{
  slot_offset_type delta = clamp_min_slot(this->trim_lower_bound_, slot_lower_bound);

  Status status = this->log_device_.trim(slot_lower_bound);
  BATT_REQUIRE_OK(status);

  return this->in_use_.spend(delta);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotWriter::halt()
{
  this->pool_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SlotWriter::WriterLock::WriterLock(SlotWriter& slot_writer) noexcept
    : slot_writer_{slot_writer}
    , writer_lock_{this->slot_writer_.log_writer_}  // Lock the LogDevice::Writer mutex
{
  BATT_CHECK_NOT_NULLPTR(*this->writer_lock_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<MutableBuffer> SlotWriter::WriterLock::prepare(usize slot_payload_size,
                                                        const batt::Grant& caller_grant) noexcept
{
  BATT_CHECK_NE(slot_payload_size, 0);

  const usize slot_header_size = packed_sizeof_varint(slot_payload_size);
  const usize slot_size = slot_header_size + slot_payload_size;
  const usize prepare_size = this->deferred_commit_size_ + slot_size;

  if (caller_grant.size() < prepare_size) {
    return ::llfs::make_status(StatusCode::kSlotGrantTooSmall);
  }

  BATT_ASSIGN_OK_RESULT(MutableBuffer buffer,
                        (*this->writer_lock_)->prepare(prepare_size, /*head_room=*/0));

  this->prepare_size_ = slot_size;

  buffer += this->deferred_commit_size_;

  Optional<MutableBuffer> payload_buffer = pack_varint_to(buffer, slot_payload_size);
  BATT_CHECK(payload_buffer);

  return {*payload_buffer};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotRange SlotWriter::WriterLock::defer_commit() noexcept
{
  const slot_offset_type slot_lower_bound = (*this->writer_lock_)->slot_offset();
  const slot_offset_type slot_upper_bound = slot_lower_bound + this->prepare_size_;

  this->deferred_commit_size_ += this->prepare_size_;
  this->prepare_size_ = 0;

  return SlotRange{
      .lower_bound = slot_lower_bound,
      .upper_bound = slot_upper_bound,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> SlotWriter::WriterLock::commit(batt::Grant& caller_grant) noexcept
{
  this->defer_commit();

  const slot_offset_type slot_lower_bound = (*this->writer_lock_)->slot_offset();

  if (this->deferred_commit_size_ == 0) {
    return SlotRange{
        .lower_bound = slot_lower_bound,
        .upper_bound = slot_lower_bound,
    };
  }

  StatusOr<batt::Grant> commit_grant = caller_grant.spend(this->deferred_commit_size_);
  if (commit_grant.status() == batt::StatusCode::kGrantUnavailable) {
    return ::llfs::make_status(StatusCode::kSlotGrantTooSmall);
  }
  BATT_REQUIRE_OK(commit_grant);

  StatusOr<slot_offset_type> slot_upper_bound =
      (*this->writer_lock_)->commit(this->deferred_commit_size_);

  BATT_REQUIRE_OK(slot_upper_bound);

  LLFS_VLOG(1) << (void*)this << " commit succeeded; new upper_bound= " << *slot_upper_bound
               << " == " << (*this->writer_lock_)->slot_offset();

  // Grow the in-use grant by the amount written.
  //
  BATT_CHECK_EQ(this->slot_writer_.in_use_.get_issuer(), commit_grant->get_issuer());
  this->slot_writer_.in_use_.subsume(std::move(*commit_grant));

  this->deferred_commit_size_ = 0;

  return SlotRange{
      .lower_bound = slot_lower_bound,
      .upper_bound = *slot_upper_bound,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotWriter::WriterLock::cancel_prepare() noexcept
{
  this->prepare_size_ = 0;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotWriter::WriterLock::cancel_all() noexcept
{
  this->cancel_prepare();
  this->deferred_commit_size_ = 0;
}

}  // namespace llfs
