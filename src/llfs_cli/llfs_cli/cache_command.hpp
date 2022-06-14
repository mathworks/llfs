#pragma once
#ifndef LLFS_CLI_CACHE_COMMAND_HPP
#define LLFS_CLI_CACHE_COMMAND_HPP

#include <boost/uuid/uuid_io.hpp>

#include <CLI/App.hpp>

#include <string>

#include <llfs_cli/arena_command.hpp>

namespace llfs_cli {

CLI::App* add_cache_command(CLI::App* app);

struct CacheCommandArgs {
  std::string filename;
  std::vector<std::string> arena_filenames_to_add;
};

void cache_create(CacheCommandArgs& args);
void cache_info(CacheCommandArgs& args);
void cache_add_arena(CacheCommandArgs& args);
void cache_add_new_arena(CacheCommandArgs& cache_args, ArenaCommandArgs& arena_args);

}  // namespace llfs_cli

#endif  // LLFS_CLI_CACHE_COMMAND_HPP
