//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/file_log_driver.hpp>

namespace llfs {

void FileLogDriver::ConcurrentSharedState::halt()
{
  this->trim_pos.close();
  this->flush_pos.close();
  this->commit_pos.close();
  this->segments.close();
}

}  // namespace llfs
