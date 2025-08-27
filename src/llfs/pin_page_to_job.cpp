//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/pin_page_to_job.hpp>
//

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool bool_from(PinPageToJob pin_page, bool default_value)
{
  switch (pin_page) {
    case PinPageToJob::kFalse:
      return false;

    case PinPageToJob::kTrue:
      return true;

    case PinPageToJob::kDefault:
      return default_value;

    default:
      BATT_PANIC() << "bad value for pin_page: " << (int)pin_page;
      BATT_UNREACHABLE();
  }
}

}  //namespace llfs
