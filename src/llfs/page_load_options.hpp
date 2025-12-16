//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_LOAD_OPTIONS_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_cache_overcommit.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/pin_page_to_job.hpp>

#include <batteries/type_traits.hpp>
#include <batteries/typed_args.hpp>

#include <memory>

namespace llfs {

/** \brief Parameters that control how a page is loaded.
 */
struct PageLoadOptions {
  using Self = PageLoadOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief The default is not to require a specific data layout.
   */
  Optional<PageLayoutId> required_layout_ = None;

  /** \brief The default is to let the loader/job decide whether to pin the page to itself.
   */
  PinPageToJob pin_page_to_job_ = PinPageToJob::kDefault;

  /** \brief The default is higher verbosity for diagnostics if the page is not found.
   */
  OkIfNotFound ok_if_not_found_ = OkIfNotFound{false};

  /** \brief The default is to set the latest use counter of the loaded page's cache slot to 1.
   */
  LruPriority lru_priority_ = LruPriority{1};

  /** \brief Whether to allow cache over-commit for this load.
   */
  PageCacheOvercommit* overcommit_ = std::addressof(PageCacheOvercommit::not_allowed());

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a PageLoadOptions with default values.
   */
  PageLoadOptions() = default;

  /** \brief Constructs a copy of the given options.
   */
  PageLoadOptions(const PageLoadOptions&) = default;

  /** \brief Copies the passed options to *this, overwriting all set values.
   */
  PageLoadOptions& operator=(const PageLoadOptions&) = default;

  /** \brief Constructs a PageLoadOptions object from typed arguments; these may be in any order.
   */
  template <typename... Args, typename = batt::EnableIfNoShadow<Self, Args...>>
  explicit PageLoadOptions(Args&&... args) noexcept;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a copy of this.
   */
  Self clone() const
  {
    return *this;
  }

  //----- --- -- -  -  -   -

  /** \brief Requires that the loaded page be of the given data layout.
   */
  Self& required_layout(const Optional<PageLayoutId>& value)
  {
    this->required_layout_ = value;
    return *this;
  }

  /** \brief Returns the required data layout for the page.
   */
  const Optional<PageLayoutId>& required_layout() const
  {
    return this->required_layout_;
  }

  //----- --- -- -  -  -   -

  /** \brief Sets whether the page should be pinned to the conetxt of the loader/job.
   */
  Self& pin_page_to_job(PinPageToJob value)
  {
    this->pin_page_to_job_ = value;
    return *this;
  }

  /** \brief Sets whether the page should be pinned to the conetxt of the loader/job.
   */
  Self& pin_page_to_job(bool value)
  {
    this->pin_page_to_job_ = value ? PinPageToJob::kTrue : PinPageToJob::kFalse;
    return *this;
  }

  /** \brief Returns whether the page is to be pinned to the context of the loader/job.
   */
  PinPageToJob pin_page_to_job() const
  {
    return this->pin_page_to_job_;
  }

  //----- --- -- -  -  -   -

  /** \brief Sets whether the caller considers it valid for the page not to be found; this controls
   * the verbosity of error diagnostics.
   */
  Self& ok_if_not_found(bool value)
  {
    this->ok_if_not_found_ = OkIfNotFound{value};
    return *this;
  }

  /** \brief Returns whether to print extra diagnostics if the page is not found.
   */
  OkIfNotFound ok_if_not_found() const
  {
    return this->ok_if_not_found_;
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

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
template <typename... Args, typename>
inline /*explicit*/ PageLoadOptions::PageLoadOptions(Args&&... args) noexcept
    : required_layout_{batt::get_typed_arg<Optional<PageLayoutId>>(None, args...)}
    , pin_page_to_job_{batt::get_typed_arg<PinPageToJob>(PinPageToJob::kDefault, args...)}
    , ok_if_not_found_{batt::get_typed_arg<OkIfNotFound>(OkIfNotFound{false}, args...)}
    , lru_priority_{batt::get_typed_arg<LruPriority>(LruPriority{1}, args...)}
    , overcommit_{std::addressof(
          batt::get_typed_arg<PageCacheOvercommit&>(PageCacheOvercommit::not_allowed(), args...))}
{
}

}  //namespace llfs
