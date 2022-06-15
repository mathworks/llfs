//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CRC_HPP
#define LLFS_CRC_HPP

#include <boost/crc.hpp>

namespace llfs {

boost::crc_basic<64> make_crc64();

}  // namespace llfs

#endif  // LLFS_CRC_HPP
