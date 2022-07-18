//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/slot_lock_manager.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotLockManager::SlotLockManager() noexcept
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotLockManager::~SlotLockManager() noexcept
{
  this->halt();

  auto locked = this->state_.lock();

  BATT_CHECK(locked->lock_heap_.empty());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool SlotLockManager::is_closed() const
{
  return this->lower_bound_.is_closed();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotLockManager::halt()
{
  this->lower_bound_.close();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type SlotLockManager::get_lower_bound() const
{
  return this->lower_bound_.get_value();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
slot_offset_type SlotLockManager::get_upper_bound() const
{
  auto locked = this->state_.lock();
  return locked->upper_bound_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<slot_offset_type> SlotLockManager::await_lower_bound(slot_offset_type min_offset)
{
  return await_slot_offset(min_offset, this->lower_bound_);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotLockManager::update_upper_bound(slot_offset_type offset)
{
  auto locked = this->state_.lock();

  this->update_upper_bound_locked(locked, /*new_upper_bound=*/offset);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotReadLock> SlotLockManager::lock_slots(const SlotRange& range, const char* holder)
{
  (void)holder;

  auto locked = this->state_.lock();

  if (range.lower_bound < this->lower_bound_.get_value()) {
    return Status{
        batt::StatusCode::kOutOfRange};  // TODO [tastolfi 2021-10-20]   "the requested value
                                         // extends below the current locked slot range"
  }

  const usize size_before = locked->lock_heap_.size();

  SlotLockHeap::handle_type handle =
      locked->lock_heap_.push(SlotLockRecord{range.lower_bound, holder});

  this->update_lower_bound_locked(locked);
  this->update_upper_bound_locked(locked, range.upper_bound);

  BATT_CHECK_EQ(size_before + 1, locked->lock_heap_.size());

  return SlotReadLock{/*sponsor=*/this, range, handle};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotLockManager::unlock_slots(SlotReadLock* read_lock)
{
  auto locked = this->state_.lock();

  const usize size_before = locked->lock_heap_.size();
  BATT_CHECK_GT(size_before, 0u);

  locked->lock_heap_.erase(read_lock->release());

  this->update_lower_bound_locked(locked);
  if (read_lock->is_upper_bound_updated()) {
    this->update_upper_bound_locked(locked, read_lock->slot_range().upper_bound);
  }

  BATT_CHECK_EQ(size_before - 1, locked->lock_heap_.size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<SlotReadLock> SlotLockManager::update_lock(SlotReadLock old_lock,
                                                    const SlotRange& new_range, const char* holder)
{
  BATT_CHECK_EQ(this, old_lock.get_sponsor()) << BATT_INSPECT(holder);
  BATT_CHECK_GE(new_range.lower_bound, old_lock.slot_range().lower_bound)
      << "The locked lower bound must increase monotonically" << BATT_INSPECT(holder);

  auto locked = this->state_.lock();

  const usize size_before = locked->lock_heap_.size();

  auto handle = old_lock.release();
  locked->lock_heap_.update(handle, SlotLockRecord{new_range.lower_bound, holder});

  this->update_lower_bound_locked(locked);
  this->update_upper_bound_locked(locked, new_range.upper_bound);

  BATT_CHECK_EQ(size_before, locked->lock_heap_.size());

  return SlotReadLock{/*sponsor=*/this, new_range, handle};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> SlotLockManager::debug_info()
{
  auto locked = this->state_.lock();

  return this->debug_info_locked(locked);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::function<void(std::ostream&)> SlotLockManager::debug_info_locked(
    batt::Mutex<State>::Lock& locked)
{
  Optional<SlotLockRecord> top_copy;
  if (!locked->lock_heap_.empty()) {
    top_copy = locked->lock_heap_.top();
  }
  std::vector<SlotLockRecord> locked_slots_copy(locked->lock_heap_.ordered_begin(),
                                                locked->lock_heap_.ordered_end());

  return [locked_slots_copy = std::move(locked_slots_copy),
          lower_bound_copy = this->lower_bound_.get_value(), top_copy](std::ostream& out) {
    out << "SlotLockManager{.acquired=[";
    {
      int limit = 10;
      for (const auto& slots : locked_slots_copy) {
        out << " " << slots << ",";
        if (--limit < 1) {
          out << " ...";
          const usize n = locked_slots_copy.size();
          for (usize i = n - 3; i < n; ++i) {
            out << " " << locked_slots_copy[i] << ",";
          }
          break;
        }
      }
    }
    out << "; size=" << locked_slots_copy.size() << "], .top=" << top_copy << "}";
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotLockManager::update_lower_bound_locked(batt::Mutex<State>::Lock& locked)
{
  if (!locked->lock_heap_.empty()) {
    const slot_offset_type trim_pos = get_slot_offset(locked->lock_heap_.top());
    LLFS_DVLOG(1) << BATT_INSPECT((void*)this) << BATT_INSPECT(trim_pos);
    BATT_CHECK_GE(trim_pos, this->lower_bound_.get_value())
        << "the locked lower bound must never move backwards!";
    this->lower_bound_.set_value(trim_pos);
  } else {
    this->lower_bound_.modify([&locked](slot_offset_type old_lower_bound) {
      return slot_max(locked->upper_bound_, old_lower_bound);
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SlotLockManager::update_upper_bound_locked(batt::Mutex<State>::Lock& locked,
                                                slot_offset_type new_upper_bound)
{
  locked->upper_bound_ = slot_max(new_upper_bound, locked->upper_bound_);
  if (locked->lock_heap_.empty()) {
    this->lower_bound_.modify([new_upper_bound](slot_offset_type old_lower_bound) {
      return slot_max(new_upper_bound, old_lower_bound);
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
SlotReadLock SlotLockManager::clone_lock(const SlotReadLock* lock) /*override*/
{
  BATT_CHECK_EQ(lock->get_sponsor(), this);

  StatusOr<SlotReadLock> cloned = this->lock_slots(lock->slot_range(), "clone_lock");
  BATT_CHECK_OK(cloned);

  return std::move(*cloned);
}

}  // namespace llfs
