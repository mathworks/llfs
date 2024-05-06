//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_IORING_LOG_DRIVER_FWD_HPP
#define LLFS_IORING_LOG_DRIVER_FWD_HPP

#include <llfs/config.hpp>

namespace llfs {

#ifndef LLFS_DISABLE_IO_URING

template <template <typename> class FlushOpImpl, typename StorageT>
class BasicIoRingLogDriver;

#endif  // LLFS_DISABLE_IO_URING

}  //namespace llfs

#endif  // LLFS_IORING_LOG_DRIVER_FWD_HPP
