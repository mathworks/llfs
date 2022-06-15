//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_STATUS_HPP
#define LLFS_STATUS_HPP

#include <llfs/status_code.hpp>

#include <batteries/status.hpp>

namespace llfs {

using batt::OkStatus;
using batt::Status;
using batt::status_from_errno;
using batt::status_from_retval;
using batt::StatusOr;

}  // namespace llfs

#endif  // LLFS_STATUS_HPP
