//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_ALLOCATOR_STATE_NO_LOCK_IPP
#define LLFS_PAGE_ALLOCATOR_STATE_NO_LOCK_IPP

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename HandlerArg>
inline void PageAllocatorStateNoLock::async_wait_free_page(HandlerArg&& handler)
{
  using HandlerFn = std::decay_t<HandlerArg>;

  static_assert(std::is_same_v<HandlerFn, HandlerArg>);

  class Wrapper
  {
   public:
    explicit Wrapper(HandlerArg&& handler, PageAllocatorStateNoLock* state) noexcept
        : handler_{BATT_FORWARD(handler)}
        , state_{state}
    {
    }

    Wrapper(const Wrapper&) = default;
    Wrapper& operator=(const Wrapper&) = default;

    //----- --- -- -  -  -   -

    void operator()(StatusOr<u64> result)
    {
      if (result.ok() && *result == 0) {
        PageAllocatorStateNoLock* const local_state = this->state_;
        local_state->free_pool_size_.async_wait(/*last_seen=*/0, std::move(*this));
      } else {
        HandlerFn local_handler = BATT_FORWARD(this->handler_);
        BATT_FORWARD(local_handler)(result.status());
      }
    }

    //----- --- -- -  -  -   -
   private:
    HandlerFn handler_;
    PageAllocatorStateNoLock* state_;
  };

  Wrapper wrapper{BATT_FORWARD(handler), this};

  wrapper(StatusOr<u64>{0});
}

}  //namespace llfs

#endif  // LLFS_PAGE_ALLOCATOR_STATE_NO_LOCK_IPP
