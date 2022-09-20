//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_USER_DATA_HPP
#define LLFS_USER_DATA_HPP

#include <llfs/int_types.hpp>

#include <batteries/assert.hpp>
#include <batteries/cpu_align.hpp>
#include <batteries/static_assert.hpp>

#include <type_traits>

namespace llfs {

usize null_user_data_key_id();
usize next_unique_user_data_key_id();

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

  explicit UserDataKey() noexcept : id_{next_unique_user_data_key_id()}
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

class                             //
    __attribute__((packed))       //
    __attribute__((aligned(64)))  //
    UserData
{
 public:
  static constexpr usize kSize = batt::kCpuCacheLineSize;
  static constexpr usize kAlignment = kSize;
  static constexpr usize kOverhead = sizeof(usize) + sizeof(UserDataDestructorFn*);
  static constexpr usize kStorageSize = kSize - kOverhead;

  UserData() = default;
  UserData(const UserData&) = delete;
  UserData& operator=(const UserData&) = delete;

  ~UserData() noexcept
  {
    this->clear();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  bool empty() const noexcept
  {
    return this->destructor_fn_ == nullptr;
  }

  void clear() noexcept
  {
    if (this->destructor_fn_) {
      this->destructor_fn_(&this->storage_);
      this->key_id_ = null_user_data_key_id();
      this->destructor_fn_ = nullptr;
    }
  }

  usize key_id() const noexcept
  {
    return this->key_id_;
  }

  template <typename T, typename... Args>
  T& emplace(const UserDataKey<T>& key, Args&&... args) noexcept
  {
    BATT_STATIC_ASSERT_LE(sizeof(T), UserData::kStorageSize);

    this->clear();
    this->destructor_fn_ = &UserDataKey<T>::destroy_user_data;
    T* obj = new (&this->storage_) T(BATT_FORWARD(args)...);
    this->key_id_ = key.id();
    return *obj;
  }

  template <typename T>
  T& get_or_panic(const UserDataKey<T>& key) noexcept
  {
    if (BATT_HINT_FALSE(key.id() != this->key_id_)) {
      BATT_PANIC() << "Wrong key id!";
    }

    return *((T*)&this->storage_);
  }

  template <typename T>
  T* get(const UserDataKey<T>& key) noexcept
  {
    if (BATT_HINT_FALSE(key.id() != this->key_id_)) {
      return nullptr;
    }

    return (T*)&this->storage_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
 private:
  std::aligned_storage_t<kStorageSize> storage_;
  usize key_id_ = null_user_data_key_id();
  UserDataDestructorFn* destructor_fn_ = nullptr;
};

}  // namespace llfs

#endif  // LLFS_USER_DATA_HPP
