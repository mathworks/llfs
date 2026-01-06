//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_ALLOCATE_OPTIONS_HPP

#include <llfs/config.hpp>
//
#include <llfs/optional.hpp>
#include <llfs/page_cache_insert_options.hpp>
#include <llfs/page_layout_id.hpp>

#include <batteries/async/cancel_token.hpp>
#include <batteries/async/types.hpp>

#include <batteries/operators.hpp>

#include <memory>

namespace llfs {

struct PageAllocateOptions : PageCacheInsertOptions {
  using Self = PageAllocateOptions;
  using Super = PageCacheInsertOptions;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static const batt::CancelToken& no_cancel_token()
  {
    static const batt::CancelToken token_{None};
    return token_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Optional<PageLayoutId> page_layout_id_ = None;

  batt::WaitForResource wait_for_resource_ = batt::WaitForResource::kFalse;

  const batt::CancelToken* cancel_token_ = std::addressof(Self::no_cancel_token());

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Constructs a PageAllocateOptions with default values.
   */
  PageAllocateOptions() = default;

  /** \brief Constructs a copy of the given options.
   */
  PageAllocateOptions(const PageAllocateOptions&) = default;

  /** \brief Copies the passed options to *this, overwriting all set values.
   */
  PageAllocateOptions& operator=(const PageAllocateOptions&) = default;

  /** \brief Constructs a PageAllocateOptions object from typed arguments; these may be in any
   * order.
   */
  template <typename... Args, typename = batt::EnableIfNoShadow<Self, Args...>>
  explicit PageAllocateOptions(Args&&... args) noexcept
      : Super{BATT_FORWARD(args)...}
      , page_layout_id_{batt::get_typed_arg(Optional<PageLayoutId>{None}, BATT_FORWARD(args)...)}
      , wait_for_resource_{batt::get_typed_arg(batt::WaitForResource::kFalse,
                                               BATT_FORWARD(args)...)}
      , cancel_token_{std::addressof(batt::get_typed_arg<const batt::CancelToken&>(
            Self::no_cancel_token(), BATT_FORWARD(args)...))}
  {
    static_assert(decltype(batt::has_typed_arg<PageLayoutId>(BATT_FORWARD(args)...)){});
    static_assert(decltype(batt::has_typed_arg<batt::WaitForResource>(BATT_FORWARD(args)...)){});
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  /** \brief Returns a copy of this.
   */
  Self clone() const
  {
    return *this;
  }

  //----- --- -- -  -  -   -

  Self& page_layout_id(const Optional<PageLayoutId>& opt_layout)
  {
    this->page_layout_id_ = opt_layout;
    return *this;
  }

  const PageLayoutId& page_layout_id() const
  {
    BATT_CHECK(this->page_layout_id_);
    return *this->page_layout_id_;
  }

  //----- --- -- -  -  -   -

  Self& wait_for_resource(batt::WaitForResource val)
  {
    this->wait_for_resource_ = val;
    return *this;
  }

  batt::WaitForResource wait_for_resource() const
  {
    return this->wait_for_resource_;
  }

  //----- --- -- -  -  -   -

  Self& cancel_token(const batt::CancelToken& val)
  {
    this->cancel_token_ = std::addressof(val);
    return *this;
  }

  const batt::CancelToken& cancel_token() const
  {
    return *this->cancel_token_;
  }
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

BATT_OBJECT_PRINT_IMPL((inline), PageAllocateOptions,
                       (wait_for_resource(), page_size(), page_layout_id(), lru_priority()))

}  // namespace llfs
