//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PIN_PAGE_TO_JOB_HPP
#define LLFS_PIN_PAGE_TO_JOB_HPP

#include <llfs/config.hpp>
//
#include <llfs/api_types.hpp>
#include <llfs/int_types.hpp>

namespace llfs {

/** \brief Controls page cache pinning behavior when loading a page.
 */
enum struct PinPageToJob : u8 {
  kFalse = 0,
  kTrue = 1,
  kDefault = 2,
};

/** \brief Convert `pin_page` to a boolean value.
 */
bool bool_from(PinPageToJob pin_page, bool default_value);

}  //namespace llfs

#endif  // LLFS_PIN_PAGE_TO_JOB_HPP
