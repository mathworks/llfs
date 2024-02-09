//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#ifndef LLFS_PAGE_CACHE_SLOT_HPP
#error This file must be included from/after page_cache_slot.hpp!
#endif

namespace llfs {

class PageCacheSlot::AtomicRef : public boost::equality_comparable<PageCacheSlot::AtomicRef>
{
 public:
  AtomicRef() = default;

  /*implicit*/ AtomicRef(const PageCacheSlot::PinnedRef& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
  }

  /*implicit*/ AtomicRef(PageCacheSlot::PinnedRef&& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
    pinned.reset();
  }

  AtomicRef(const AtomicRef& that) noexcept : slot_{detail::increment_weak_ref(that.slot_.load())}
  {
  }

  AtomicRef(AtomicRef&& that) noexcept : slot_{that.slot_.exchange(nullptr)}
  {
  }

  ~AtomicRef() noexcept
  {
    detail::decrement_weak_ref(this->slot_.load());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  void unsynchronized_swap(AtomicRef& that) noexcept
  {
    this->slot_.store(that.slot_.exchange(this->slot_.load()));
  }

  AtomicRef& operator=(const AtomicRef& that) noexcept
  {
    AtomicRef copy{that};
    copy.unsynchronized_swap(*this);
    return *this;
  }

  AtomicRef& operator=(AtomicRef&& that) noexcept
  {
    AtomicRef copy{std::move(that)};
    copy.unsynchronized_swap(*this);
    return *this;
  }

  explicit operator bool() const noexcept
  {
    return this->slot_.load() != nullptr;
  }

  PageCacheSlot::PinnedRef pin(PageId key) const noexcept
  {
    auto* slot = this->slot_.load();
    if (!slot) {
      return {};
    }
    return slot->acquire_pin(key);
  }

  PageCacheSlot* slot() const noexcept
  {
    return this->slot_.load();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::atomic<PageCacheSlot*> slot_{nullptr};
};

}  //namespace llfs
