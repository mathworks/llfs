//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/user_data.hpp>
//

#include <atomic>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize null_user_data_key_id()
{
  static const UserDataKey<void> null_key_;
  return null_key_.id();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize next_unique_user_data_key_id()
{
  static std::atomic<usize> id_{0};
  return id_.fetch_add(1);
}

}  // namespace llfs
