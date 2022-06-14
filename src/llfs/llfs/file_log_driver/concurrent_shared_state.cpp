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
