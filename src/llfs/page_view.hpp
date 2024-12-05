//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_VIEW_HPP
#define LLFS_PAGE_VIEW_HPP

#include <llfs/api_types.hpp>
#include <llfs/key.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_filter.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout.hpp>
//#include <llfs/page_loader.hpp>
#include <llfs/seq.hpp>
#include <llfs/stable_string_store.hpp>
#include <llfs/user_data.hpp>

#include <batteries/async/mutex.hpp>
#include <batteries/status.hpp>

#include <memory>
#include <type_traits>

namespace llfs {

class PageView
{
 public:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageView(std::shared_ptr<const PageBuffer>&& data) noexcept
      : data_{std::move(data)}
      , user_data_{}
  {
    BATT_STATIC_ASSERT_EQ(sizeof(UserData), UserData::kSize);
  }

  PageView(const PageView&) = delete;
  PageView& operator=(const PageView&) = delete;

  virtual ~PageView() = default;

  const PackedPageHeader& header() const
  {
    return ::llfs::get_page_header(*this->data_);
  }

  PageId page_id() const noexcept
  {
    return this->data_->page_id();
  }

  std::shared_ptr<const PageBuffer> data() const noexcept
  {
    return this->data_;
  }

  ConstBuffer const_buffer() const noexcept
  {
    return this->data_->const_buffer();
  }

  ConstBuffer const_payload() const noexcept
  {
    return this->data_->const_payload();
  }

  // Check to make sure the page id matches the expected one, and that no data corruption is found.
  //
  Status validate(PageId expected_id);

  /** \brief Get the tag for this page view.
   */
  virtual PageLayoutId get_page_layout_id() const = 0;

  /** \brief Returns a sequence of the ids of all pages directly referenced by this one.
   */
  virtual BoxedSeq<PageId> trace_refs() const = 0;
  //
  //  TODO [tastolfi 2023-03-13] add parallel friendly API

  /** \brief Returns the minimum key value contained within this page.
   */
  virtual Optional<KeyView> min_key() const = 0;

  /** \brief Returns the maximum key value contained within this page.
   */
  virtual Optional<KeyView> max_key() const = 0;

  /** \brief Retrieves at most the size of `key_buffer_out` number of keys contained in this page.
   *
   * \param lower_bound This parameter allows for "skipping" to an arbitrary place in the page's key
   * set. The caller should provide an index (offset) into the key set, which represents the
   * starting key from which this function will collect keys from to return.
   *
   * \param key_buffer_out The output buffer that will be filled by this function with the requested
   * keys.
   *
   * \param storage A `StableStringStore` instance that the caller can provide so that the returned
   * keys can still be a list of `KeyView` even if the keys in the page are stored in a way that
   * isn't contiguous or are compressed. Specific implementations of `PageView` will choose to use
   * this based on their key storage.
   *
   * \return The number of keys filled into `key_buffer_out`. This value will either be the size of
   * `key_buffer_out` or the number of keys between `lower_bound` and the end of the key set,
   * whichever is smaller. In the event that the `lower_bound` parameter provided is out of the
   * range of the key set, this function will return 0.
   */
  virtual StatusOr<usize> get_keys(ItemOffset lower_bound, const Slice<KeyView>& key_buffer_out,
                                   StableStringStore& storage) const;

  // Builds a key-based approximate member query (AMQ) filter for the page, to answer the question
  // whether a given key *might* be contained by the page.
  //
  virtual std::shared_ptr<PageFilter> build_filter() const = 0;

  // Dump a human-readable representation or summary of the page to the passed stream.
  //
  virtual void dump_to_ostream(std::ostream& out) const = 0;

  // Get typed user data for the specified key, if that is the active key for this PageView.
  // Otherwise, return `None`.
  //
  template <typename T>
  Optional<T> get_user_data(const UserDataKey<T>& key) const
  {
    Optional<T> value;
    {
      auto locked = this->user_data_.lock();
      T* ptr = locked->get(key);
      if (ptr != nullptr) {
        value.emplace(*ptr);
      }
    }
    return value;
  }

  // Set the user data associated with this Page iff there is not already user data present under
  // the given key.  `make_value` must have signature: `T make_value()`.
  //
  template <typename T, typename MakeValueFn>
  T set_user_data(const UserDataKey<T>& key, MakeValueFn&& make_value) const
  {
    auto locked = this->user_data_.lock();
    T* ptr = locked->get(key);
    if (BATT_HINT_TRUE(ptr)) {
      return *ptr;
    }
    return locked->emplace(key, BATT_FORWARD(make_value)());
  }

  // Like `set_user_data`, but never overwrites existing data stored under a different key (instead
  // returns None in this case).
  //
  template <
      typename T, typename MakeValueFn,
      typename = std::enable_if_t<std::is_constructible_v<T, std::invoke_result_t<MakeValueFn>>>>
  Optional<T> init_user_data(const UserDataKey<T>& key, MakeValueFn&& make_value) const
  {
    auto locked = this->user_data_.lock();
    T* ptr = locked->get(key);
    if (BATT_HINT_TRUE(ptr)) {
      return *ptr;
    }
    if (BATT_HINT_FALSE(!locked->empty())) {
      return None;
    }
    return locked->emplace(key, BATT_FORWARD(make_value)());
  }

  template <
      typename T, typename MakeValueFn,
      typename = std::enable_if_t<!std::is_constructible_v<T, std::invoke_result_t<MakeValueFn>> &&
                                  batt::IsStatusOr<std::invoke_result_t<MakeValueFn>>{}>,
      typename = void>
  StatusOr<T> init_user_data(const UserDataKey<T>& key, MakeValueFn&& make_value) const
  {
    auto locked = this->user_data_.lock();
    T* ptr = locked->get(key);
    if (BATT_HINT_TRUE(ptr)) {
      return *ptr;
    }
    if (BATT_HINT_FALSE(!locked->empty())) {
      return make_status(StatusCode::kPageViewUserDataAlreadyInitialized);
    }
    auto status_or_value = BATT_FORWARD(make_value)();
    BATT_REQUIRE_OK(status_or_value);
    return locked->emplace(key, std::move(*status_or_value));
  }

 private:
  std::shared_ptr<const PageBuffer> data_;
  mutable batt::Mutex<UserData> user_data_;
  //            ^
  //            TODO [tastolfi 2021-12-01] potential concurrency bottleneck
};

}  // namespace llfs

#endif  // LLFS_PAGE_VIEW_HPP
