//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_sequencer.hpp>
//

namespace llfs {

namespace {

Optional<SlotRange> poll_result_to_optional(StatusOr<SlotRange>&& result)
{
  if (result.ok()) {
    return *result;
  }

  if (result.status() == batt::StatusCode::kUnavailable) {
    return None;
  }

  BATT_PANIC() << "Latch::poll() must return ok or unavailable!";
  BATT_UNREACHABLE();
}

}  // namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool SlotSequencer::has_prev() const
{
  return this->prev_ != nullptr;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool SlotSequencer::is_resolved() const
{
  return this->current_->is_ready();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<SlotRange> SlotSequencer::poll_prev() const
{
  if (this->prev_ == nullptr) {
    return SlotRange{0, 0};
  }
  return poll_result_to_optional(this->prev_->poll());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotRange> SlotSequencer::await_prev() const
{
  if (this->prev_ == nullptr) {
    return SlotRange{0, 0};
  }

  return this->prev_->await();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<SlotRange> SlotSequencer::get_current() const
{
  if (this->current_ == nullptr) {
    return None;
  }
  return poll_result_to_optional(this->current_->poll());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool SlotSequencer::set_current(const SlotRange& slot_range)
{
  return this->current_->set_value(slot_range);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool SlotSequencer::set_error(batt::Status status)
{
  return this->current_->set_value(std::move(status));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotSequencer SlotSequencer::get_next() const
{
  SlotSequencer next;
  next.prev_ = this->current_;

  return next;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> SlotSequencer::debug_info() const
{
  return [this](std::ostream& out) {
    out << "SlotSequencer{.prev_=" << (const void*)this->prev_.get() << ", .current=_"
        << (const void*)this->current_.get() << ",}";
  };
}

}  // namespace llfs
