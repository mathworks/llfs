//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_CLI_ARENA_COMMAND_HPP
#define LLFS_CLI_ARENA_COMMAND_HPP

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <CLI/App.hpp>

#include <llfs/int_types.hpp>

#include <boost/uuid/random_generator.hpp>

#include <string>

namespace llfs_cli {

using namespace llfs::int_types;

CLI::App* add_arena_command(CLI::App* app);

struct ArenaCommandArgs {
  std::string filename;
  u64 base_offset = 0;
  u64 page_count = 0;
  u64 page_size = 0;
  u64 max_attachments = 64;
  u64 total_size = 0;
  boost::uuids::uuid device_uuid = boost::uuids::random_generator{}();
  u32 device_id = 0;
};

void arena_create(ArenaCommandArgs& args);
void arena_info(ArenaCommandArgs& args);

void add_arena_create_options(CLI::App* app, std::shared_ptr<ArenaCommandArgs>&& args);

}  // namespace llfs_cli

#endif  // LLFS_CLI_ARENA_COMMAND_HPP
