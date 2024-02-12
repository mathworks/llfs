//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/lru_clock.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LRUClock::LocalCounter::LocalCounter() noexcept : value{0}
{
  batt::ScopedLock<LocalCounterList> locked{LRUClock::instance().counter_list_};
  locked->push_back(*this);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LRUClock::LocalCounter::~LocalCounter() noexcept
{
  batt::ScopedLock<LocalCounterList> locked{LRUClock::instance().counter_list_};
  locked->erase(locked->iterator_to(*this));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto LRUClock::instance() noexcept -> Self&
{
  // Leak instance_ to avoid shutdown destructor ordering issues.
  //
  static Self* instance_ = new Self;

  return *instance_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ LRUClock::LocalCounter& LRUClock::thread_local_counter() noexcept
{
  thread_local LocalCounter counter_;

  return counter_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ i64 LRUClock::read_local() noexcept
{
  return Self::thread_local_counter().value.load();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ i64 LRUClock::advance_local() noexcept
{
  return Self::thread_local_counter().value.fetch_add(1);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ i64 LRUClock::read_global() noexcept
{
  batt::ScopedLock<LocalCounterList> locked{Self::instance().counter_list_};

  return Self::instance().observed_count_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
LRUClock::LRUClock() noexcept
    : sync_thread_{[this] {
      this->run();
    }}
{
  this->sync_thread_.detach();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void LRUClock::run() noexcept
{
  static_assert(kMinSyncDelayUsec <= kMaxSyncDelayUsec);

  std::random_device rand_dev;
  std::default_random_engine rng(rand_dev());
  std::uniform_int_distribution<i64> pick_jitter{0,
                                                 Self::kMaxSyncDelayUsec - Self::kMinSyncDelayUsec};

  // Loop forever, waiting and synchronizing thread-local counters.
  //
  for (;;) {
    // Pick a delay with random jitter.
    //
    const i64 delay_usec = Self::kMinSyncDelayUsec + pick_jitter(rng);

    // Wait...
    //
    std::this_thread::sleep_for(std::chrono::microseconds(delay_usec));

    // Synchronize the thread-local counters; this will update this->observed_count_.
    //
    this->sync_local_counters();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void LRUClock::sync_local_counters() noexcept
{
  batt::ScopedLock<LocalCounterList> locked{this->counter_list_};

  i64 max_value = this->observed_count_;

  // On the first pass, figure out the maximum counter value.
  //
  for (LocalCounter& counter : *locked) {
    max_value = std::max(max_value, counter.value.load());
  }

  // Save the observed max counter value so that we continue to advance, even if all threads
  // terminate.
  //
  this->observed_count_ = max_value;

  // On the second pass, use CAS to make sure that all local counters are at least at the
  // `max_value` calculated above.
  //
  for (LocalCounter& counter : *locked) {
    i64 observed = counter.value.load();
    while (observed < max_value) {
      if (counter.value.compare_exchange_weak(observed, max_value)) {
        break;
      }
    }
  }

  // Done!
}

}  //namespace llfs
