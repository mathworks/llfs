//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CONFIRM_HPP
#define LLFS_CONFIRM_HPP

#include <ostream>

namespace llfs {

enum struct ConfirmThisWillEraseAllMyData : bool {
  kNo = false,
  kYes = true,
};

inline std::ostream& operator<<(std::ostream& out, const ConfirmThisWillEraseAllMyData& t)
{
  return out << (bool)t;
}

}  // namespace llfs

#endif  // LLFS_CONFIRM_HPP
