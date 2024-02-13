//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_LRU_CLOCK_HPP
#define LLFS_LRU_CLOCK_HPP

#include <llfs/config.hpp>
//

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>

#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <thread>

namespace llfs {

/** \brief A fast (if slightly inaccurate) logical timestamp maintainer, suitable for comparing
 * approximate last-time-of-usage for different objects.
 *
 * The LRUClock is comprised of two elements:
 *
 *  1. A (set of) thread-local monotonic event counters
 *  2. A background task that periodically synchronizes the thread-local counters
 *
 * The thread-local event counters are atomic, but since they are almost always accessed from a
 * single thread (and we use the weakest possible memory order/fencing), they cause minimal problems
 * in terms of cache stalls and contention.  Every time a thread reads its counter, it advances it
 * by one.  Periodically (every 0.5 to 1.5 ms by default, with pseudo-random jitter), the background
 * task wakes up and cycles over all the counters, setting them each to the highest value found in
 * any of the others (it must, in the worst case, cycle through all twice).
 *
 * Every time a thread wants to use the LRU clock for the first time, it must first acquire a global
 * mutex lock in order to add its local counter to a linked-list.  The background synchronization
 * task must also grab this mutex to make sure there are no races on the list while it updates all
 * the thread-local counters.  When a thread exits, it acquires the mutex and removes its counter.
 *
 * Note: a regular thread never needs to block in order to simply acquire a logical timestamp; it
 * only needs to do so when it starts or stops.
 */
class LRUClock
{
 public:
  using Self = LRUClock;

  //----- --- -- -  -  -   -
  static constexpr i64 kMinSyncDelayUsec = 500;
  static constexpr i64 kMaxSyncDelayUsec = 1500;
  //----- --- -- -  -  -   -

  class LocalCounter;

  /** \brief A linked-list node; this is the base type for LocalCounter.
   */
  using LocalCounterHook = boost::intrusive::list_base_hook<boost::intrusive::tag<LocalCounter>>;

  /** \brief A per-thread atomic counter.
   */
  class LocalCounter : public LocalCounterHook
  {
   public:
    explicit LocalCounter() noexcept;

    ~LocalCounter() noexcept;

    std::atomic<i64> value{0};
  };

  /** \brief Alias for the counter linked-list collection type.
   */
  using LocalCounterList =
      boost::intrusive::list<LocalCounter, boost::intrusive::base_hook<LocalCounterHook>>;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a reference to the global instance of the LRUClock.
   */
  static Self& instance() noexcept;

  /** \brief Returns a reference to the current thread's counter object.
   */
  static LocalCounter& thread_local_counter() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns the current thread's local counter.
   */
  static i64 read_local() noexcept;

  /** \brief Increments the current thread's local counter, returning the old value.
   */
  static i64 advance_local() noexcept;

  /** \brief Returns the last observed maximum count (over all thread-local values); this may be
   * slightly out of date, as it is only updated by the background sync thread.
   */
  static i64 read_global() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  LRUClock() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The background counter sync thread entry point.
   */
  void run() noexcept;

  /** \brief Locks the counter list mutex, then iterates through all thread-local counters,
   * atomically updating each to be at least the maximum observed value.
   *
   * This takes two passes through the list, so it's not 100% guaranteed that all counters will be
   * the same by the end.
   */
  void sync_local_counters() noexcept;

  /** \brief Adds the passed LocalCounter to the global list.
   */
  void add_local_counter(LocalCounter& counter) noexcept;

  /** \brief Removes the passed LocalCounter from the global list.
   */
  void remove_local_counter(LocalCounter& counter) noexcept;

  /** \brief Returns the maximum count value from the last time sync_local_counters() was called.
   */
  i64 read_observed_count() noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::mutex mutex_;
  LocalCounterList counter_list_;
  std::thread sync_thread_;

  // Keeps track of the synchronized counter value as it advances, so we don't go backwards if all
  // the threads go away temporarily at some point.
  //
  i64 observed_count_ = 0;
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  //namespace llfs

#endif  // LLFS_LRU_CLOCK_HPP
