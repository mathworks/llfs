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
auto SlotWriter::prepare(batt::Grant& caller_grant, usize slot_body_size,
                         Optional<std::string_view> name) -> StatusOr<Append>
{
  BATT_CHECK_NE(slot_body_size, 0);

  const usize slot_header_size = packed_sizeof_varint(slot_body_size);
  const usize slot_size = slot_header_size + slot_body_size;

  StatusOr<batt::Grant> slot_grant = caller_grant.spend(slot_size);
  if (slot_grant.status() == batt::StatusCode::kGrantUnavailable) {
    return ::llfs::make_status(StatusCode::kSlotGrantTooSmall);
  }
  BATT_REQUIRE_OK(slot_grant);

  batt::Mutex<LogDevice::Writer*>::Lock writer_lock = this->log_writer_.lock();
  StatusOr<MutableBuffer> slot_buffer = (*writer_lock)->prepare(slot_size, /*head_room=*/0);
  BATT_REQUIRE_OK(slot_buffer);

  return {Append{
      this,
      std::move(writer_lock),
      std::move(*slot_grant),
      *slot_buffer,
      slot_body_size,
      name,
  }};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotWriter::Append::Append(SlotWriter* that, batt::Mutex<LogDevice::Writer*>::Lock writer_lock,
                           batt::Grant&& slot_grant, const MutableBuffer& slot_buffer,
                           usize slot_body_size, Optional<std::string_view> name) noexcept
    : that_{that}
    , writer_lock_{std::move(writer_lock)}
    , slot_grant_{std::move(slot_grant)}
    , cancelled_{false}
    , committed_{false}
    , slot_lower_bound_{(*this->writer_lock_)->slot_offset()}
    , packer_{slot_buffer}
{
  BATT_CHECK_NOT_NULLPTR(*this->writer_lock_);
  BATT_CHECK_EQ(this->slot_grant_.get_issuer(), &this->that_->pool_);
  BATT_CHECK_EQ(this->packer_.buffer_size(), this->slot_grant_.size());
  BATT_CHECK(this->packer_.pack_varint(slot_body_size));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotWriter::Append::~Append() noexcept
{
  this->cancel();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> SlotWriter::Append::commit()
{
  if (this->cancelled_) {
    return {batt::StatusCode::kCancelled};
  }

  BATT_CHECK(!this->committed_);
  this->committed_ = true;
  LLFS_VLOG(1) << "LogDevice::Writer::commit(" << this->packer_.buffer_size() << ")";

  StatusOr<slot_offset_type> commit_slot_upper_bound =
      (*this->writer_lock_)->commit(this->packer_.buffer_size());

  BATT_REQUIRE_OK(commit_slot_upper_bound);

  LLFS_VLOG(1) << (void*)this << " commit succeeded; new upper_bound= " << *commit_slot_upper_bound
               << " == " << (*this->writer_lock_)->slot_offset();

  // Grow the in-use grant by the amount written.
  //
  BATT_CHECK_EQ(this->that_->in_use_.get_issuer(), this->slot_grant_.get_issuer());
  this->that_->in_use_.subsume(std::move(this->slot_grant_));

  return SlotRange{
      .lower_bound = this->slot_lower_bound_,
      .upper_bound = *commit_slot_upper_bound,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotWriter::Append::cancel()
{
  const auto set_cancelled = batt::finally([&] {
    this->cancelled_ = true;
  });

  if (this->writer_lock_) {
    if (!this->cancelled_ && !this->committed_) {
      (*this->writer_lock_)->commit(0).IgnoreError();
    }
    this->slot_grant_.spend_all();
    this->writer_lock_.release();
  }
}

}  // namespace llfs
