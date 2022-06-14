#include <llfs/volume_file.hpp>
//

#include <llfs/storage_file_builder.hpp>

namespace llfs {

#if 0
//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::unique_ptr<FileOffsetPtr<PackedVolumeConfig>>> prepare_volume_file(  //
    const VolumeOptions& options,                                                  //
    const VolumeFileOptions& file_options                                          //
)
{
  auto p_config = std::make_unique<FileOffsetPtr<PackedVolumeConfig>>();

  FileLayoutPlanner planner{file_options.base_file_offset.value_or(0)};

  StatusOr<FileOffsetPtr<PackedVolumeConfig&>> planned_volume =
      planner.append_volume(p_config->object, options, file_options);

  BATT_REQUIRE_OK(planned_volume);

  p_config->file_offset = planned_volume->file_offset;

  return p_config;
}
#endif

}  // namespace llfs
