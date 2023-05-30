//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_driver.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring_log_driver.ipp>

namespace llfs {

template class BasicIoRingLogDriver<BasicIoRingLogFlushOp>;

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
