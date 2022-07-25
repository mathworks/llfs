//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs_cli/list_command.hpp>
//

#include <llfs/ioring.hpp>
#include <llfs/raw_block_file_impl.hpp>
#include <llfs/storage_file.hpp>

#include <boost/uuid/uuid_io.hpp>

#include <iomanip>

namespace llfs_cli {

using namespace llfs;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
CLI::App* add_list_command(CLI::App* cmd)
{
  CLI::App* list_cmd = cmd->add_subcommand("list", "List contents of storage files (.llfs)");

  auto args = std::make_shared<ListCommandArgs>();

  list_cmd->add_option("files", args->files, "Files whose contents to list.");

  list_cmd->callback([args] {
    run_list_command(*args);
  });

  return list_cmd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void run_list_command(ListCommandArgs& args)
{
  StatusOr<ScopedIoRing> ioring = ScopedIoRing::make_new(MaxQueueDepth{32}, ThreadPoolSize{1});
  BATT_CHECK_OK(ioring);

  std::cout << std::endl;

  for (auto f : args.files) {
    std::cout << f << ":" << std::endl;

    StatusOr<std::unique_ptr<IoRingRawBlockFile>> file =
        IoRingRawBlockFile::open(ioring->get(), f.c_str(), /*flags=*/O_RDONLY, /*mode=*/None);
    BATT_CHECK_OK(file);

    StatusOr<std::vector<std::unique_ptr<StorageFileConfigBlock>>> config_blocks =
        read_storage_file(**file, /*start_offset=*/0);
    BATT_CHECK_OK(config_blocks);

    for (const auto& block : *config_blocks) {
      for (const PackedConfigSlot& slot : block->get_const().slots) {
        std::cout << std::setw(16) << std::setfill(' ')
                  << PackedConfigSlotBase::Tag::to_string(slot.tag) << " " << slot.uuid
                  << std::endl;
      }
    }
    std::cout << std::endl;
  }
}

}  // namespace llfs_cli
