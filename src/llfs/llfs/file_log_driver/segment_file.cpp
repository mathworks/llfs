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
