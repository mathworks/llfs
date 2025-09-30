//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_DEVICE_PAIRING_HPP

#include <llfs/int_types.hpp>
//
#include <llfs/optional.hpp>

#include <batteries/assert.hpp>

#include <atomic>
#include <ostream>
#include <string>
#include <string_view>

namespace llfs {

inline constexpr i32 kMaxPageDevicePairings = 4;

/** \brief An id that specifies a role for paired PageDevices.
 *
 * Paired PageDevices allow many pages on different devices, possibly of differing sizes, to share a
 * lifetime with a primary page on a given device.  For example, this can be used to implement
 * various replication schemes, or to implement AMQ filtering on a per-page basis.
 */
class PageDevicePairing
{
 public:
  using Self = PageDevicePairing;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  explicit PageDevicePairing(std::string_view name,
                             Optional<std::string_view> description = None) noexcept
      : value_{Self::next_id_value()}
      , name_{name}
      , description_{description.value_or(name)}
  {
  }

  PageDevicePairing(const PageDevicePairing&) = delete;
  PageDevicePairing& operator=(const PageDevicePairing&) = delete;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  auto operator<=>(const Self&) const = default;

  i32 value() const
  {
    return this->value_;
  }

  const std::string& name() const
  {
    return this->name_;
  }

  const std::string& description() const
  {
    return this->description_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   --
 private:
  static i32 next_id_value() noexcept
  {
    static std::atomic<i32> value_{0};

    const i32 next_id = value_.fetch_add(1);

    BATT_CHECK_LT(next_id, kMaxPageDevicePairings)
        << "Too many instances of PageDevicePairing; please increase"
        << BATT_INSPECT(kMaxPageDevicePairings);

    return next_id;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i32 value_;
  std::string name_;
  std::string description_;
};

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
inline std::ostream& operator<<(std::ostream& out, const PageDevicePairing& t)
{
  return out << "PageDevicePairing{" << t.value() << ", .name=" << t.name()
             << ", .description=" << t.description() << "}";
}

}  //namespace llfs
