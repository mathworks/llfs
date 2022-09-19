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
#include <batteries/status.hpp>

#include <memory>
#include <type_traits>

namespace llfs {

class PageView
{
 public:
  static usize null_user_data_key_id();
  static usize next_unique_user_data_key_id();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  using UserDataDestructorFn = void(void*) noexcept;

  template <typename T>
  class UserDataKey
  {
   public:
    static_assert(std::is_same_v<std::decay_t<T>, T>,
                  "UserDataKey may only be used with non-ref, non-qualified types");

    static void destroy_user_data(void* ptr) noexcept
    {
      T* typed_ptr = (T*)ptr;
      typed_ptr->~T();
    }

    //+++++++++++-+-+--+----- --- -- -  -  -   -

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

  struct __attribute__((packed)) __attribute__((aligned(64))) UserData {
    static constexpr usize kSize = batt::kCpuCacheLineSize;
    static constexpr usize kAlignment = kSize;
    static constexpr usize kOverhead = sizeof(usize) + sizeof(UserDataDestructorFn*);
    static constexpr usize kStorageSize = kSize - kOverhead;

    UserData() = default;
    UserData(const UserData&) = delete;
    UserData& operator=(const UserData&) = delete;

    std::aligned_storage_t<kStorageSize> storage;
    usize key_id{PageView::null_user_data_key_id()};
    UserDataDestructorFn* destructor_fn = nullptr;

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    // TODO [tastolfi 2022-09-17]
    //
    // New Design:
    //
    // - Make UserData cpu cache line aligned, equal in size to a cache line
    // - Add C fn ptr for dtor of storage - make it a template instantiation
    // - value is aligned_storage_t taking up the remainder of space
    // - (in downstream code) call set_user_data when the PageView is constructed
    //
    //+++++++++++-+-+--+----- --- -- -  -  -   -

    bool empty() const noexcept
    {
      return this->destructor_fn == nullptr;
    }

    template <typename T, typename... Args>
    T& emplace(const UserDataKey<T>& key, Args&&... args) noexcept
    {
      BATT_STATIC_ASSERT_LE(sizeof(T), UserData::kStorageSize);

      if (this->destructor_fn) {
        this->destructor_fn(&this->storage);
      }
      this->destructor_fn = &UserDataKey<T>::destroy_user_data;
      T* obj = new (&this->storage) T(BATT_FORWARD(args)...);
      this->key_id = key.id();
      return *obj;
    }

    template <typename T>
    T& get_or_panic(const UserDataKey<T>& key) const noexcept
    {
      if (BATT_HINT_FALSE(key.id() != this->key_id)) {
        BATT_PANIC() << "Wrong key id!";
      }

      return *((T*)&this->storage);
    }

    template <typename T>
    T* get(const UserDataKey<T>& key) const noexcept
    {
      if (BATT_HINT_FALSE(key.id() != this->key_id)) {
        return nullptr;
      }

      return (T*)&this->storage;
    }
  };

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
