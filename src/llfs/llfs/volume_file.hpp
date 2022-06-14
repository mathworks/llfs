#pragma once
#ifndef LLFS_VOLUME_FILE_HPP
#define LLFS_VOLUME_FILE_HPP

#include <llfs/confirm.hpp>
#include <llfs/file_offset_ptr.hpp>
#include <llfs/optional.hpp>
#include <llfs/status.hpp>
#include <llfs/volume.hpp>
#include <llfs/volume_config.hpp>
#include <llfs/volume_options.hpp>

namespace llfs {

// Write the prepared volume to the specified file.
//
StatusOr<FileOffsetPtr<PackedVolumeConfig>> initialize_volume_file(  //
    std::string_view file_name,                                      //
    const FileOffsetPtr<PackedVolumeConfig>& packed_volume_config,   //
    ConfirmThisWillEraseAllMyData confirm                            //
);

// Recover a volume from the given file.
//
StatusOr<std::unique_ptr<Volume>> recover_volume_from_file(  //
    batt::TaskScheduler& scheduler,                          //
    batt::SharedPtr<PageCache>&& page_cache,                 //
    std::string_view file_name,                              //
    i64 volume_config_file_offset,                           //
    std::unique_ptr<SlotLockManager> trim_control,           //
    const VolumeReader::SlotVisitorFn& slot_visitor_fn       //
);

}  // namespace llfs

#endif  // LLFS_VOLUME_FILE_HPP
