//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/system_config.hpp>
//

#include <sys/mman.h>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize system_page_size()
{
  static const usize size = getpagesize();
  return size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize round_down_to_page_size_multiple(usize count)
{
  return count - (count % system_page_size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize round_up_to_page_size_multiple(usize count)
{
  return round_down_to_page_size_multiple(count + system_page_size() - 1);
}

}  // namespace llfs
