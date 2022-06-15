//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_KEY_HPP
#define LLFS_KEY_HPP

#include <llfs/interval.hpp>

#include <string>
#include <string_view>
#include <variant>

namespace llfs {

using Key = std::string;
using KeyView = std::string_view;

inline const KeyView& get_key(const KeyView& key)
{
  return key;
}

struct KeyOrder {
  template <typename L, typename R>
  bool operator()(const L& left, const R& right) const
  {
    return operator()(get_key(left), get_key(right));
  }

  bool operator()(const std::string_view& left, const std::string_view& right) const
  {
    if (left.size() < right.size()) {
      return __builtin_memcmp(left.data(), right.data(), left.size()) <= 0;
    } else {
      return __builtin_memcmp(left.data(), right.data(), right.size()) < 0;
    }
  }
};

struct KeyEqual {
  template <typename L, typename R>
  bool operator()(const L& left, const R& right) const
  {
    return get_key(left) == get_key(right);
  }
};

template <typename... Ts>
inline const KeyView& get_key(const std::variant<Ts...>& var)
{
  return std::visit(
      [](const auto& c) -> decltype(auto) {
        return get_key(c);
      },
      var);
}

struct HeapKeyOrder {
  template <typename L, typename R>
  bool operator()(const L& left, const R& right) const
  {
    return KeyOrder{}(right, left);
  }
};

struct KeyRangeOrder : KeyOrder {
  template <typename TraitsL, typename TraitsR>
  bool operator()(const BasicInterval<TraitsL>& l, const BasicInterval<TraitsR>& r) const
  {
    static_assert(interval_traits_compatible<TraitsL, TraitsR>(), "");
    return TraitsL::empty(get_key(r.lower_bound), get_key(l.upper_bound));
  }

  template <typename L, typename TraitsR>
  bool operator()(const L& l, const BasicInterval<TraitsR>& r) const
  {
    return TraitsR::x_excluded_by_lower(get_key(l), get_key(r.lower_bound));
  }

  template <typename TraitsL, typename R>
  bool operator()(const BasicInterval<TraitsL>& l, const R& r) const
  {
    return TraitsL::upper_excludes_x(get_key(l.upper_bound), get_key(r));
  }

  template <typename L, typename R>
  bool operator()(const L& l, const R& r) const
  {
    return KeyOrder::operator()(l, r);
  }
};

}  // namespace llfs

#endif  // LLFS_KEY_HPP
