//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_CACHE_INSERT_OPTIONS_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/page_cache_overcommit.hpp>
#include <llfs/page_size.hpp>

#include <batteries/bit_ops.hpp>
#include <batteries/typed_args.hpp>
#include <batteries/utility.hpp>

#include <memory>

namespace llfs {

struct PageCacheInsertOptions {
  using Self = PageCacheInsertOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The size of the page being inserted (bytes); must be a power of 2.
   */
  PageSize page_size_{0};

  /** \brief The eviction priority (lower == evict first).
   */
  LruPriority lru_priority_{1};

  /** \brief Whether to allow cache over-commit for this insertion.
   */
  PageCacheOvercommit* overcommit_ = std::addressof(PageCacheOvercommit::not_allowed());

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a PageCacheInsertOptions with default values.
   */
  PageCacheInsertOptions() = default;

  /** \brief Constructs a copy of the given options.
   */
  PageCacheInsertOptions(const PageCacheInsertOptions&) = default;

  /** \brief Copies the passed options to *this, overwriting all set values.
   */
  PageCacheInsertOptions& operator=(const PageCacheInsertOptions&) = default;

  /** \brief Constructs a PageCacheInsertOptions object from typed arguments; these may be in any
   * order.
   */
  template <typename... Args, typename = batt::EnableIfNoShadow<Self, Args...>>
  explicit PageCacheInsertOptions(Args&&... args) noexcept
      : page_size_{batt::get_typed_arg(PageSize{0}, BATT_FORWARD(args)...)}
      , lru_priority_{batt::get_typed_arg(LruPriority{1}, BATT_FORWARD(args)...)}
      , overcommit_{std::addressof(batt::get_typed_arg<PageCacheOvercommit&>(
            PageCacheOvercommit::not_allowed(), BATT_FORWARD(args)...))}
  {
    static_assert(decltype(batt::has_typed_arg<PageSize>(BATT_FORWARD(args)...)){});
    static_assert(decltype(batt::has_typed_arg<LruPriority>(BATT_FORWARD(args)...)){});
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a copy of this.
   */
  Self clone() const
  {
    return *this;
  }

  //----- --- -- -  -  -   -

  Self& page_size(u32 n_bytes)
  {
    BATT_CHECK_EQ(batt::bit_count(n_bytes), 1) << "PageSize must be a power of 2";
    this->page_size_ = PageSize{n_bytes};
    return *this;
  }

  PageSize page_size() const
  {
    return this->page_size_;
  }

  //----- --- -- -  -  -   -

  /** \brief Sets the LruPriority for updating the latest access field of the cache slot.
   *
   * This is an integer which is added to the current clock counter of the slot; each time the clock
   * hand passes the slot, this counter is decremented.  It must go to zero before the slot can be
   * evicted.  Therefore setting a higher priority value for certain slots will cause them to stay
   * in cache longer.
   */
  Self& lru_priority(i64 value)
  {
    this->lru_priority_ = LruPriority{value};
    return *this;
  }

  /** \brief Returns the requested LruPriority for the load.
   */
  LruPriority lru_priority() const
  {
    return this->lru_priority_;
  }

  //----- --- -- -  -  -   -

  /** \brief Sets the overcommit option for this load.
   */
  Self& overcommit(PageCacheOvercommit& overcommit)
  {
    this->overcommit_ = std::addressof(overcommit);
    return *this;
  }

  PageCacheOvercommit& overcommit() const
  {
    BATT_CHECK_NOT_NULLPTR(this->overcommit_);
    return *this->overcommit_;
  }
};

}  // namespace llfs
