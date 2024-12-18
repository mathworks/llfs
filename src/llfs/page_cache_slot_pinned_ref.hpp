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

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class CallerPromisesTheyAcquiredPinCount
{
 private:
  CallerPromisesTheyAcquiredPinCount() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  // The following declared "friend" functions are the only places that should be acquiring a pin on
  // a cache slot and using it to create a PinnedRef!
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  friend auto PageCacheSlot::acquire_pin(PageId key,
                                         bool ignore_key) noexcept -> PageCacheSlot::PinnedRef;

  friend auto PageCacheSlot::fill(PageId key) noexcept -> PageCacheSlot::PinnedRef;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
class PageCacheSlot::PinnedRef : public boost::equality_comparable<PageCacheSlot::PinnedRef>
{
 public:
  friend class PageCacheSlot;

  using value_type = batt::Latch<std::shared_ptr<const PageView>>;

  PinnedRef() = default;

 private:
  explicit PinnedRef(PageCacheSlot* slot, CallerPromisesTheyAcquiredPinCount) noexcept
      : slot_{slot}
      , value_{slot ? slot->value() : nullptr}
  {
  }

 public:
  PinnedRef(const PinnedRef& that) noexcept : slot_{that.slot_}, value_{that.value_}
  {
    if (this->slot_) {
      this->slot_->extend_pin();
    }
  }

  PinnedRef& operator=(const PinnedRef& that) noexcept
  {
    PinnedRef copy{that};
    this->swap(copy);
    return *this;
  }

  PinnedRef(PinnedRef&& that) noexcept : slot_{that.slot_}, value_{that.value_}
  {
    that.slot_ = nullptr;
    that.value_ = nullptr;
  }

  PinnedRef& operator=(PinnedRef&& that) noexcept
  {
    PinnedRef copy{std::move(that)};
    this->swap(copy);
    return *this;
  }

  ~PinnedRef() noexcept
  {
    this->reset();
  }

  void reset()
  {
    if (this->slot_) {
      this->slot_->release_pin();
      this->slot_ = nullptr;
      this->value_ = nullptr;
    }
  }

  void swap(PinnedRef& that)
  {
    std::swap(this->slot_, that.slot_);
    std::swap(this->value_, that.value_);
  }

  explicit operator bool() const
  {
    return this->slot_ != nullptr;
  }

  PageCacheSlot* slot() const noexcept
  {
    return this->slot_;
  }

  PageId key() const noexcept
  {
    return this->slot_->key();
  }

  value_type* value() const noexcept
  {
    return this->value_;
  }

  value_type* get() const noexcept
  {
    return this->value();
  }

  value_type* operator->() const noexcept
  {
    return this->get();
  }

  value_type& operator*() const noexcept
  {
    return *this->get();
  }

  u32 pin_count() const noexcept
  {
    return this->slot_ ? this->slot_->pin_count() : 0;
  }

  u64 ref_count() const noexcept
  {
    return this->slot_ ? this->slot_->ref_count() : 0;
  }

 private:
  PageCacheSlot* slot_ = nullptr;
  value_type* value_ = nullptr;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

inline bool operator==(const PageCacheSlot::PinnedRef& l, const PageCacheSlot::PinnedRef& r)
{
  return l.slot() == r.slot() && l.get() == r.get();
}

}  //namespace llfs
