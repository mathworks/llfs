//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/file_log_driver.hpp>
//

#include <llfs/filesystem.hpp>

#include <glog/logging.h>

namespace llfs {

Status FileLogDriver::SegmentFile::remove()
{
  VLOG(1) << "trimming log segment file: " << this->file_name;
  return delete_file(this->file_name);
}

StatusOr<ConstBuffer> FileLogDriver::SegmentFile::read(MutableBuffer buffer) const
{
  return read_file(this->file_name, buffer);
}

}  // namespace llfs
