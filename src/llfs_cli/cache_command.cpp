//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <boost/uuid/uuid_io.hpp>
//

#include <llfs_cli/cache_command.hpp>
//

#include <llfs/data_packer.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/page_arena_file.hpp>
#include <llfs/page_cache_config_file.hpp>
#include <llfs/page_id_factory.hpp>

namespace llfs_cli {

using namespace llfs;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CLI::App* add_cache_command(CLI::App* cmd)
{
  CLI::App* cache_cmd = cmd->add_subcommand("cache", "Manage PageCache config (.llc) files");

  auto args = std::make_shared<CacheCommandArgs>();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    CLI::App* cache_create_cmd =
        cache_cmd->add_subcommand("create", "Create a new PaceCache config (.llc) file");

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    cache_create_cmd
        ->add_option("page_cache_config_filename,-f,--file", args->filename,
                     "The PageCache config (.llc) file")
        ->required()
        ->check(CLI::NonexistentPath);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    cache_create_cmd->callback([args] {
      cache_create(*args);
    });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    CLI::App* cache_info_cmd = cache_cmd->add_subcommand(
        "info", "Read PageCache config (.llc) file and display information");

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    cache_info_cmd
        ->add_option("page_cache_config_filename,-f,--file", args->filename,
                     "The PageCache config (.llc) file")
        ->required()
        ->check(CLI::ExistingFile);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    cache_info_cmd->callback([args] {
      cache_info(*args);
    });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    CLI::App* cache_add_arena_cmd = cache_cmd->add_subcommand(
        "add_arena",
        "Add an already-existing PageArena (.lla) file to a PageCache config (.llc) file");

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    cache_add_arena_cmd
        ->add_option("page_cache_config_filename,-f,--file", args->filename,
                     "The PageCache config (.llc) file")
        ->required()
        ->check(CLI::ExistingFile);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    cache_add_arena_cmd
        ->add_option("page_arena_filename,-a,--arena", args->arena_filenames_to_add,
                     "A comma-separated list of ")
        ->required()
        ->check(CLI::ExistingFile);

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    cache_add_arena_cmd->callback([args] {
      cache_add_arena(*args);
    });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  return cache_cmd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void cache_create(CacheCommandArgs& args)
{
  const PageCacheConfigFile empty_cache_config;

  Status status = write_page_cache_config_file(args.filename, empty_cache_config);
  BATT_CHECK_OK(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void cache_info(CacheCommandArgs& args)
{
  Status status = [&]() -> Status {
    StatusOr<PageCacheConfigFile> cache_config = read_page_cache_config_file(args.filename);
    BATT_REQUIRE_OK(cache_config);

    std::cout << batt::dump_range(cache_config->arenas, batt::Pretty::True) << std::endl;

    return OkStatus();
  }();
  BATT_CHECK_OK(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void cache_add_arena(CacheCommandArgs& args)
{
  (void)args;

  BATT_PANIC() << "Implement me!";

#if 0
  Status status = [&]() -> Status {
    StatusOr<PageCacheConfigFile> cache_config = read_page_cache_config_file(args.filename);
    BATT_REQUIRE_OK(cache_config);

    // Build a list of in-use device ids to detect and prevent conflicts.
    //
    std::set<page_device_id_int> device_ids_in_use;
    const auto check_device_id_unique = [&](page_device_id_int device_id) {
      if (device_ids_in_use.count(device_id)) {
        BATT_PANIC() << "duplicate device_id found in PageCache config! "
                     << BATT_INSPECT(device_id);
      }
      device_ids_in_use.emplace(device_id);
    };

    for (const PageArenaFileRef& arena_ref : cache_config->arenas) {
      check_device_id_unique(arena_ref.page_device_id);
    }

    // Load arena configs from all the specified files and add them to the cache config.
    //
    for (const std::string& arena_filename : args.arena_filenames_to_add) {
      StatusOr<std::vector<std::unique_ptr<PageArenaConfigInFile>>> arena_configs =
          read_arena_configs_from_file(arena_filename);

      BATT_REQUIRE_OK(arena_configs);

      for (auto& to_add : *arena_configs) {
        check_device_id_unique(to_add->config.page_device->device_id);

        cache_config->arenas.emplace_back(PageArenaFileRef{
            .arena_file = to_add->file_ref,
            .page_device_id = batt::checked_cast<u32>(to_add->config.page_device.device_id.value()),
            .page_device_uuid = to_add->config.page_device.device_uuid,
        });

        VLOG(1) << "added " << cache_config->arenas.back();
      }
    }

    // Write the new config.
    //
    return write_page_cache_config_file(args.filename, *cache_config);
  }();

  BATT_CHECK_OK(status);
#endif
}

}  // namespace llfs_cli
