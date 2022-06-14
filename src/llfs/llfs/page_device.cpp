#include <llfs/page_device.hpp>
//

#include <batteries/async/task.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto PageDevice::await_read(PageId id) -> ReadResult
{
  return batt::Task::await<ReadResult>([id, this](auto&& handler) {
    this->read(id, BATT_FORWARD(handler));
  });
}

}  // namespace llfs
