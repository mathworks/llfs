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

#include <llfs/logging.hpp>
#include <llfs/status_code.hpp>

#include <batteries/optional.hpp>
#include <batteries/status.hpp>

#include <boost/preprocessor/cat.hpp>
#include <boost/preprocessor/stringize.hpp>

namespace llfs {

using batt::OkStatus;
using batt::Status;
using batt::status_from_errno;
using batt::status_from_retval;
using batt::StatusOr;

#define LLFS_WARN_IF_NOT_OK(expr)                                                                  \
  for (auto BOOST_PP_CAT(llfs_TmpStatusResult, __LINE__) = ::batt::make_optional((expr));          \
       BATT_HINT_FALSE(BOOST_PP_CAT(llfs_TmpStatusResult, __LINE__) &&                             \
                       !BOOST_PP_CAT(llfs_TmpStatusResult, __LINE__)->ok());                       \
       BOOST_PP_CAT(llfs_TmpStatusResult, __LINE__) = ::batt::None)                                \
  LLFS_LOG_WARNING() << "Expected OK result, but got: \n\n"                                        \
                     << BOOST_PP_STRINGIZE((expr)) << " == "                                       \
                                                   << BOOST_PP_CAT(llfs_TmpStatusResult, __LINE__) \
                                                   << "\n\n"

}  // namespace llfs

#endif  // LLFS_STATUS_HPP
