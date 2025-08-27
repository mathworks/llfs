//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_LOADER_HPP
#define LLFS_PAGE_LOADER_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/int_types.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout_id.hpp>
#include <llfs/page_loader_decl.hpp>
#include <llfs/pin_page_to_job.hpp>
#include <llfs/status.hpp>

#include <batteries/type_traits.hpp>
#include <batteries/typed_args.hpp>

namespace llfs {

class PageCache;

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
  explicit PageLoadOptions(Args&&... args) noexcept
      : required_layout_{batt::get_typed_arg<Optional<PageLayoutId>>(None, args...)}
      , pin_page_to_job_{batt::get_typed_arg<PinPageToJob>(PinPageToJob::kDefault, args...)}
      , ok_if_not_found_{batt::get_typed_arg<OkIfNotFound>(OkIfNotFound{false}, args...)}
      , lru_priority_{batt::get_typed_arg<LruPriority>(LruPriority{1}, args...)}
  {
  }

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
};

/** \brief Interface for an entity which can resolve `PageId`s into `PinnedPage`s.
 */
template <typename PinnedPageParamT>
class BasicPageLoader
{
 public:
  /** \brief A movable and copyable type representing a pin on a loaded page.
   */
  using PinnedPageT = PinnedPageParamT;

  /** \brief PageLoaders are not copyable.
   */
  BasicPageLoader(const BasicPageLoader&) = delete;

  /** \brief PageLoaders are not copyable.
   */
  BasicPageLoader& operator=(const BasicPageLoader&) = delete;

  virtual ~BasicPageLoader() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a pointer to the PageCache object associated with this loader.
   */
  virtual PageCache* page_cache() const = 0;

  /** \brief Provides a hint to the loader that the given page is likely to be loaded in the near
   * future.
   *
   * PageLoader implementations do not need to do anything with this information; however, a
   * reasonable implementation might be to start a background load of the given page if there is
   * extra bandwidth available in the underyling storage device.
   */
  virtual void prefetch_hint(PageId page_id) = 0;

  /** \brief Attempts to pin the given page in the cache; guaranteed not to block or perform I/O.
   *
   * If the specified page is not currently in the cache, this function returns
   * batt::StatusCode::kUnavailable.
   */
  virtual StatusOr<PinnedPageT> try_pin_cached_page(PageId page_id,
                                                    const PageLoadOptions& options) = 0;

  /** \brief Pins and returns the given page, loading it if it is not yet in cache.
   *
   * This function may block the caller while performing I/O.
   */
  virtual StatusOr<PinnedPageT> load_page(PageId page_id, const PageLoadOptions& options) = 0;

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 protected:
  BasicPageLoader() = default;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
// NOTE: PageLoader is a type alias defined in `<llfs/page_loader_decl.hpp>`.
//
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

//=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

}  // namespace llfs

#endif  // LLFS_PAGE_LOADER_HPP
