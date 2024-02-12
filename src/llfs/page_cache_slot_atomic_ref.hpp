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

#include <boost/operators.hpp>

#include <atomic>

namespace llfs {

/** \brief A lock-free, thread-safe weak reference to a PageCacheSlot object.
 */
class PageCacheSlot::AtomicRef : public boost::equality_comparable<PageCacheSlot::AtomicRef>
{
 public:
  /** \brief Constructs an empty (invalid) AtomicRef.
   */
  AtomicRef() = default;

  /** \brief Initializes a new AtomicRef from an existing pinned ref.
   *
   * This does not affect the pin count of the slot.
   */
  /*implicit*/ AtomicRef(const PageCacheSlot::PinnedRef& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
  }

  /** \brief Initializes a new AtomicRef from an existing pinned ref.
   *
   * This will release the passed pinned ref, decrementing the pin count by one.
   */
  /*implicit*/ AtomicRef(PageCacheSlot::PinnedRef&& pinned) noexcept
      : slot_{detail::increment_weak_ref(pinned.slot())}
  {
    pinned.reset();
  }

  /** \brief Initializes a new AtomicRef by copying an existing one.
   *
   * This will increment the slot's weak ref count, but leave the pin count unchanged.
   */
  AtomicRef(const AtomicRef& that) noexcept : slot_{detail::increment_weak_ref(that.slot_.load())}
  {
  }

  /** \brief Initializes a new AtomicRef by moving an existing one to a new object.
   *
   * This leaves both the slot's weak ref count and pin count unchanged.  The passed AtomicRef
   * (that) is cleared.
   */
  AtomicRef(AtomicRef&& that) noexcept : slot_{that.slot_.exchange(nullptr)}
  {
  }

  /** \brief If this ref is valid, decrements the weak ref count of the slot.
   */
  ~AtomicRef() noexcept
  {
    detail::decrement_weak_ref(this->slot_.load());
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Swaps the values of `this` and `that`.
   *
   * Because the swap operation cannot be implemented with a single atomic instruction, this
   * function MUST only be used when the caller is sure that there is no concurrent access to
   * `this`.  Observers of `that` will see a single atomic transition to the new value (the old
   * value of this).
   */
  void unsynchronized_swap(AtomicRef& that) noexcept
  {
    this->slot_.store(that.slot_.exchange(this->slot_.load()));
  }

  /** \brief Assigns this to that, returning a reference to this.
   *
   * This is safe to call concurrently for the same AtomicRef object; however, observers of `this`
   * and `that` will see the change to the value of each as two distinct atomic events.
   */
  AtomicRef& operator=(const AtomicRef& that) noexcept
  {
    AtomicRef copy{that};
    copy.unsynchronized_swap(*this);
    return *this;
  }

  /** \brief Assigns this to that, returning a reference to this.
   *
   * Clears the value of `that`.
   *
   * This is safe to call concurrently for the same AtomicRef object; however, observers of `this`
   * and `that` will see the change to the value of each as two distinct atomic events.
   */
  AtomicRef& operator=(AtomicRef&& that) noexcept
  {
    AtomicRef copy{std::move(that)};
    copy.unsynchronized_swap(*this);
    return *this;
  }

  /** \brief Returns true iff this is a valid ref (non-null).
   */
  explicit operator bool() const noexcept
  {
    return this->slot_.load() != nullptr;
  }

  /** \brief Attempts to pin the referenced slot, returning a valid PinnedRef iff successful.
   *
   * If this AtomicRef does not refer to a slot (i.e., it is invalid), then this will return an
   * invalid PinnedRef to indicate failure.  If the pin fails for any reason, e.g. the slot has been
   * evicted and/or its current key is different from `key`, returns an invalid PinnedRef.
   */
  PageCacheSlot::PinnedRef pin(PageId key) const noexcept
  {
    auto* slot = this->slot_.load();
    if (!slot) {
      return {};
    }
    return slot->acquire_pin(key);
  }

  /** \brief Returns a pointer to the referenced slot (nullptr if this reference is currently
   * invalid).
   */
  PageCacheSlot* slot() const noexcept
  {
    return this->slot_.load();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::atomic<PageCacheSlot*> slot_{nullptr};
};

}  //namespace llfs
