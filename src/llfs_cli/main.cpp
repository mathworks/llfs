//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

//######=###=##=#=#=#=#=#==#==#====#+==#+==============+==+==+==+=+==+=+=+=+=+=+=+
// LLFS Command-Line Interface.
//

#include <llfs_cli/list_command.hpp>

#include <CLI/App.hpp>
#include <CLI/Config.hpp>
#include <CLI/Formatter.hpp>

#include <llfs/config.hpp>

#include <iostream>

int main(int argc, char** argv)
{
  CLI::App app{"Low-Level File System (LLFS) Command Line Utility"};

  llfs_cli::add_list_command(&app);

  app.require_subcommand();

  CLI11_PARSE(app, argc, argv);

  return 0;
}
