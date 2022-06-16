//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_METHOD_BINDER_HPP
#define LLFS_METHOD_BINDER_HPP

namespace llfs {
}  // namespace llfs

#define LLFS_METHOD_BINDER(type, method, binder)                                                   \
  class binder                                                                                     \
  {                                                                                                \
   public:                                                                                         \
    binder() = default;                                                                            \
                                                                                                   \
    explicit binder(type& obj) noexcept : p_obj_{&obj}                                             \
    {                                                                                              \
      BATT_ASSERT_NOT_NULLPTR(p_obj_);                                                             \
      ++p_obj_->binder_count;                                                                      \
    }                                                                                              \
                                                                                                   \
    binder(const binder& that) noexcept : p_obj_{that.p_obj_}                                      \
    {                                                                                              \
      if (p_obj_) {                                                                                \
        ++p_obj_->binder_count;                                                                    \
      }                                                                                            \
    }                                                                                              \
                                                                                                   \
    binder& operator=(const binder& that) noexcept                                                 \
    {                                                                                              \
      if (p_obj_) {                                                                                \
        BATT_CHECK_GT(p_obj_->binder_count, 0);                                                    \
        --p_obj_->binder_count;                                                                    \
      }                                                                                            \
      p_obj_ = that.p_obj_;                                                                        \
      if (p_obj_) {                                                                                \
        ++p_obj_->binder_count;                                                                    \
      }                                                                                            \
      return *this;                                                                                \
    }                                                                                              \
                                                                                                   \
    ~binder() noexcept                                                                             \
    {                                                                                              \
      if (p_obj_) {                                                                                \
        --p_obj_->binder_count;                                                                    \
      }                                                                                            \
    }                                                                                              \
                                                                                                   \
    template <typename... Args>                                                                    \
    decltype(auto) operator()(Args&&... args) const                                                \
    {                                                                                              \
      BATT_ASSERT_NOT_NULLPTR(p_obj_);                                                             \
      return p_obj_->method(std::forward<Args>(args)...);                                          \
    }                                                                                              \
                                                                                                   \
   private:                                                                                        \
    type* p_obj_ = nullptr;                                                                        \
  }

#endif  // LLFS_METHOD_BINDER_HPP
