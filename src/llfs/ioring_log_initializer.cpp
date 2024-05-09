//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_initializer.hpp>
//

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/ioring_log_initializer.ipp>
#include <llfs/logging.hpp>

#include <batteries/metrics/metric_collectors.hpp>

namespace llfs {

template class BasicIoRingLogInitializer<IoRing>;

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status initialize_ioring_log_device(RawBlockFile& file, const IoRingLogConfig& config,
                                    ConfirmThisWillEraseAllMyData /*confirm*/)
{
  LLFS_VLOG(1) << "initializing IoRingLogDevice; " << BATT_INSPECT(config.block_count())
               << BATT_INSPECT(config.block_size()) << BATT_INSPECT(config.block_capacity());

  batt::LatencyMetric block_write_latency;
  {
    batt::LatencyTimer block_write_timer{block_write_latency, config.block_count()};

    IoRing::File* ioring_file = file.get_io_ring_file();

    const u64 n_blocks_to_init = [&] {
      if (kFastIoRingLogDeviceInit) {
        return u64{1};
      }
      return config.block_count();
    }();

    if (ioring_file) {
      IoRingLogInitializer initializer{/*n_tasks=*/std::min<usize>(1024, n_blocks_to_init),
                                       *ioring_file, config, n_blocks_to_init};

      batt::Status init_status = initializer.run();

      BATT_REQUIRE_OK(init_status);

    } else {
      LLFS_LOG_INFO_FIRST_N(10) << "Using slow path for log device initialization";

      PackedLogPageBuffer buffer;
      buffer.clear();
      buffer.header.reset();

      u64 file_offset = config.physical_offset;
      for (u64 block_i = 0; block_i < n_blocks_to_init; ++block_i) {
        LLFS_VLOG(2) << "writing initial block header; " << BATT_INSPECT(buffer.header.slot_offset)
                     << BATT_INSPECT(file_offset);
        Status write_status = write_all(file, file_offset, buffer.as_const_buffer());
        BATT_REQUIRE_OK(write_status);
        buffer.header.slot_offset += config.block_capacity();
        file_offset += config.block_size();
      }
    }
  }  // LatencyTimer

  LLFS_VLOG(1) << "Success! " << block_write_latency.rate_per_second() << " blocks/sec";

  return OkStatus();
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
