//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CLI_LIST_COMMAND_HPP
#define LLFS_CLI_LIST_COMMAND_HPP

#include <CLI/App.hpp>

#include <llfs/int_types.hpp>

#include <string>
#include <vector>

namespace llfs_cli {

using namespace llfs::int_types;

CLI::App* add_list_command(CLI::App* app);

struct ListCommandArgs {
  std::vector<std::string> files;
};

void run_list_command(ListCommandArgs& args);

}  // namespace llfs_cli

#endif  // LLFS_CLI_LIST_COMMAND_HPP
