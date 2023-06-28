//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/fuse.hpp>

#include "mem_fuse.hpp"

#include <iostream>

int main(int argc, char* argv[])
{
  struct stat st;
  std::memset(&st, 0, sizeof(st));

  int rt = lstat(".", &st);

  std::cout << std::endl << llfs::DumpStat{st} << BATT_INSPECT(rt) << std::endl << std::endl;

  batt::StatusOr<llfs::FuseSession> session =
      llfs::FuseSession::from_args(argc, argv, batt::StaticType<llfs::MemoryFuseImpl>{});

  BATT_CHECK_OK(session);

  return session->run();
}
