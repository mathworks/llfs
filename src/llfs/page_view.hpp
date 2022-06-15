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

#include <llfs/key.hpp>
#include <llfs/page_buffer.hpp>
#include <llfs/page_filter.hpp>
#include <llfs/page_id.hpp>
#include <llfs/page_layout.hpp>
#include <llfs/page_loader.hpp>
#include <llfs/seq.hpp>

#include <batteries/async/mutex.hpp>

#include <memory>

namespace llfs {

class PageView
{
 public:
  static usize null_user_data_key_id();
  static usize next_unique_user_data_key_id();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  template <typename T>
  class UserDataKey
  {
   public:
    static_assert(std::is_same_v<std::decay_t<T>, T>,
                  "UserDataKey may only be used with non-ref, non-qualified types");

    explicit UserDataKey() noexcept : id_{PageView::next_unique_user_data_key_id()}
    {
    }

    usize id() const
    {
      return this->id_;
    }

   private:
    usize id_;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  struct UserData {
    usize key_id{PageView::null_user_data_key_id()};
    std::any value;
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageView(std::shared_ptr<const PageBuffer>&& data) noexcept
      : data_{std::move(data)}
      , user_data_{}
  {
  }

  PageView(const PageView&) = delete;
  PageView& operator=(const PageView&) = delete;

  virtual ~PageView() = default;

  PageId page_id() const
  {
    return this->data_->page_id();
  }

  std::shared_ptr<const PageBuffer> data() const
  {
    return this->data_;
  }

  // Check to make sure the page id matches the expected one, and that no data corruption is found.
  //
  Status validate(PageId expected_id);

  // Get the tag for this page view.
  //
  virtual PageLayoutId get_page_layout_id() const = 0;

  // Returns a sequence of the ids of all pages directly referenced by this one.
  //
  virtual BoxedSeq<PageId> trace_refs() const = 0;

  // Returns the minimum key value contained within this page.
  //
  virtual Optional<KeyView> min_key() const = 0;

  // Returns the maximum key value contained within this page.
  //
  virtual Optional<KeyView> max_key() const = 0;

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
      if (locked->key_id == key.id()) {
        value.emplace(std::any_cast<T>(locked->value));
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
    if (locked->key_id != key.id()) {
      locked->value = BATT_FORWARD(make_value)();
    }
    return std::any_cast<T>(locked->value);
  }

 private:
  std::shared_ptr<const PageBuffer> data_;
  mutable batt::Mutex<UserData> user_data_;
  //            ^
  //            TODO [tastolfi 2021-12-01] potential concurrency bottleneck
};

}  // namespace llfs

#endif  // LLFS_PAGE_VIEW_HPP
