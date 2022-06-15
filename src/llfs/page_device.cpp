//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

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
