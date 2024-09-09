//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SLOT_LOCK_MANAGER_HPP
#define LLFS_SLOT_LOCK_MANAGER_HPP

#include <llfs/slot_read_lock.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/async/watch.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SlotLockManager : public SlotReadLock::Sponsor
{
 public:
  SlotLockManager() noexcept;

  ~SlotLockManager() noexcept;

  /** \brief Returns true if `halt()` has been called.
   */
  bool is_closed() const;

  /** \brief Closes all watch objects owned by the lock manager.
   */
  void halt();

  /** \brief Return the current locked range lower bound.
   *
   * This value is guaranteed to increase monotonically.  A consequence of this invariant is that
   * any attempt to lock a slot range that extends below the current locked range will fail.
   */
  slot_offset_type get_lower_bound() const;

  /** \brief Return the current locked range upper bound.
   */
  slot_offset_type get_upper_bound() const;

  /** \brief Convenience wrapper: SlotRange{this->get_lower_bound(), this->get_upper_bound()}.
   */
  SlotRange get_locked_range() const
  {
    return SlotRange{this->get_lower_bound(), this->get_upper_bound()};
  }

  /** \brief Blocks the current task until the locked lower bound is at least `min_offset`.  This
   * may happen either due to updating the upper bound or because a lock is released (see below for
   * details).
   */
  StatusOr<slot_offset_type> await_lower_bound(slot_offset_type min_offset);

  /** \brief Updates the locked upper bound to be the greater of `offset` and its current value.
   *
   * If there are no active locks, this has the side-effect of also updating the locked lower bound
   * (to maintain the invariant that the locked range is empty when no locks are held).
   *
   * The upper bound is guranteed to increase monotonically.
   */
  void update_upper_bound(slot_offset_type offset);

  /** \brief Acquire a lock on the given slot range, if it does not extend below the current locked
   * range.
   *
   * If `range.lower_bound` is lower than the current locked lower bound, the operation fails and an
   * error Status is returned.
   *
   * If `range.upper_bound` is greater than the current locked upper bound, then the locked upper
   * bound is set to `range.upper_bound`.
   *
   * When the last moved copy of the returned SlotReadLock is destroyed, the lock is released and
   * the locked interval is shrunk.
   */
  StatusOr<SlotReadLock> lock_slots(const SlotRange& range, const char* holder);

  /** \brief Atomically reads the slot lower bound and acquires/returns a lock at that
   * offset.
   *
   * This function creates a lock whose lower and upper bounds are equal; thus neither bound (in the
   * manager) is modified by acquiring and this lock.
   */
  StatusOr<SlotReadLock> lock_all_slots(const char* holder);

  /** \brief Efficiently updates an existing lock by changing the range to a greater one.
   */
  StatusOr<SlotReadLock> update_lock(SlotReadLock old_lock, const SlotRange& new_range,
                                     const char* holder);

  /** \brief For debugging.
   */
  std::function<void(std::ostream&)> debug_info();

  /** \brief Clone a lock.
   */
  SlotReadLock clone_lock(const SlotReadLock* lock) override;

 private:
  struct State {
    // All active locks.
    //
    SlotLockHeap lock_heap_;

    // The current slot upper bound.
    //
    slot_offset_type upper_bound_ = 0;

    //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  };

  /** \brief Implements the core logic of acquiring a SlotReadLock.  Does not check to make sure the
   * range is valid; the caller must make sure it is or behavior is undefined!
   */
  StatusOr<SlotReadLock> lock_slots_nocheck(batt::ScopedLock<State>& locked, const SlotRange& range,
                                            const char* holder);

  std::function<void(std::ostream&)> debug_info_locked(batt::ScopedLock<State>& locked);

  void unlock_slots(SlotReadLock*) override;

  void update_lower_bound_locked(batt::ScopedLock<State>& locked);

  void update_upper_bound_locked(batt::ScopedLock<State>& locked, slot_offset_type new_upper_bound);

  mutable batt::Mutex<State> state_;
  batt::Watch<slot_offset_type> lower_bound_{0};
};

}  // namespace llfs

#endif  // LLFS_SLOT_LOCK_MANAGER_HPP
