//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <boost/uuid/uuid_io.hpp>
//

#include <llfs_cli/arena_command.hpp>
//

#include <llfs/filesystem.hpp>
#include <llfs/page_arena_file.hpp>

#include <batteries/assert.hpp>

#include <iostream>

namespace llfs_cli {

using namespace llfs;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CLI::App* add_arena_command(CLI::App* cmd)
{
  CLI::App* arena_cmd = cmd->add_subcommand("arena", "Manage PageArena (.lla) files");

  auto args = std::make_shared<ArenaCommandArgs>();

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    CLI::App* arena_create_cmd =
        arena_cmd->add_subcommand("create", "Create a new PageArena (.lla) file");

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    arena_create_cmd
        ->add_option("page_arena_filename,-f,--file", args->filename, "The PageArena (.lla) file")
        ->required()
        ->check(CLI::NonexistentPath);

    add_arena_create_options(arena_create_cmd, batt::make_copy(args));

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    arena_create_cmd->add_option("-i,--id", args->device_id, "Serial id number for device")
        ->required();

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    arena_create_cmd->callback([args] {
      arena_create(*args);
    });
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  {
    CLI::App* arena_info_cmd =
        arena_cmd->add_subcommand("info", "Print information about PageArena (.lla) file");

    //+++++++++++-+-+--+----- --- -- -  -  -   -
    arena_info_cmd->add_option("filename,-f,--file", args->filename, "The PageArena (.lla) file")
        ->required()
        ->check(CLI::ExistingFile);

    arena_info_cmd->callback([args] {
      arena_info(*args);
    });
  }

  return arena_cmd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void add_arena_create_options(CLI::App* app, std::shared_ptr<ArenaCommandArgs>&& args)
{
  //+++++++++++-+-+--+----- --- -- -  -  -   -
  CLI::Option* page_count_opt =
      app->add_option("-n,--page_count", args->page_count, "The number of pages")
          ->transform(CLI::AsSizeValue(true));

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  app->add_option("-b,--page_size", args->page_size, "The page size in bytes")
      ->required()
      ->transform(CLI::AsSizeValue(false));

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  app->add_option("-t,--total_size", args->total_size, "The total arena size")
      ->transform(CLI::AsSizeValue(false))
      ->excludes(page_count_opt);

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  app->add_option("-m,--max_attachments", args->max_attachments,
                  "The maximum number of client attachments supported");

  //+++++++++++-+-+--+----- --- -- -  -  -   -
  app->add_option("-u,--uuid", args->device_uuid, "UUID for the device");
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void arena_create(ArenaCommandArgs& args)
{
  if (args.page_count == 0 && args.page_size == 0) {
    std::cerr << "Error: at least one of {page_count, page_size} must be specified!" << std::endl;
  }
  if (args.page_count == 0) {
    BATT_CHECK_NE(args.total_size, 0u);
    args.page_count = (args.total_size + args.page_size - 1) / args.page_size;
  } else if (args.page_size == 0) {
    BATT_CHECK_NE(args.total_size, 0u);
    args.page_size = 1ull << batt::log2_ceil(args.total_size / args.page_count);
  } else {
    BATT_CHECK_EQ(args.total_size, 0u);
    args.total_size = args.page_count * args.page_size;
  }

  VLOG(1) << BATT_INSPECT(args.page_count);
  VLOG(1) << BATT_INSPECT(args.page_size);
  VLOG(1) << BATT_INSPECT(args.total_size);

  BATT_PANIC() << "Implement me!";

#if 0
  auto builder = PackedPageArenaConfig::Builder{}
                     .page_count(args.page_count)
                     .page_size(args.page_size)
                     .max_attach_count(args.max_attachments)
                     .device_id(args.device_id)
                     .device_uuid(args.device_uuid);

  PackedPageArenaConfig config;
  BATT_CHECK_OK(builder.build(&config));

  VLOG(1) << BATT_INSPECT(config);

  // Create an empty file of the required size.
  //
  BATT_CHECK_OK(truncate_file(args.filename, config.total_arena_size()));

  VLOG(1) << BATT_INSPECT(config.total_arena_size()) << BATT_INSPECT(sizeof(config));

  FileSegmentRef file_ref{
      .path_utf8 = args.filename,
      .offset = 0,
      .size = config.total_arena_size(),
  };

  StatusOr<IoRing> ioring = IoRing::make_new(/*entries=*/256);
  BATT_CHECK_OK(ioring);

  ioring->on_work_started();

  // The IoRing requires a background thread to process events.
  //
  std::thread ioring_thread{[&] {
    VLOG(1) << "ioring_thread entered";
    Status ioring_exit_status = ioring->run();
    VLOG(1) << BATT_INSPECT(ioring_exit_status);
    VLOG(1) << "ioring_thread finished";
  }};

  // Initialize the page arena file.
  //
  {
    PageArenaFileRuntimeOptions runtime_options;
    StatusOr<PageArena> arena =
        initialize_page_arena_file(batt::Runtime::instance().default_scheduler(), *ioring, file_ref,
                                   runtime_options, config, ConfirmThisWillEraseAllMyData::kYes);
    VLOG(1) << "arena initialize returned: " << arena.status();
    BATT_CHECK_OK(arena);
  }
  ioring->on_work_finished();

  // Shut down the IoRing.
  //
  VLOG(1) << "stopping the ioring";
  ioring->stop();

  VLOG(1) << "joining the ioring_thread";
  ioring_thread.join();
#endif
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void arena_info(ArenaCommandArgs& args)
{
  StatusOr<std::vector<std::unique_ptr<PageArenaConfigInFile>>> records =
      read_arena_configs_from_file(args.filename);
  BATT_CHECK_OK(records);

  if (records->empty()) {
    std::cerr << "No PackedPageArenaConfig found at the end of '" << args.filename
              << "'; is this a valid PageArena (.lla) file?" << std::endl;
    return;
  }

  for (auto& r : *records) {
    std::cout << r->file_ref << std::endl << r->config << std::endl;
  }
}

}  // namespace llfs_cli
