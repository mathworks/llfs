//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/file_log_driver.hpp>

#include <llfs/logging.hpp>

namespace llfs {

FileLogDriver::FlushTaskMain::FlushTaskMain(const RingBuffer& buffer,
                                            ConcurrentSharedState& shared_state,
                                            ActiveFile&& active_file) noexcept
    : buffer_{buffer}
    , shared_state_{shared_state}
    , active_file_{std::move(active_file)}
{
}

void FileLogDriver::FlushTaskMain::operator()()
{
  const Status status = [&] {
    // Do as much work as we can, grabbing the latest flush/commit position values so that we don't
    // sleep unless we absolutely have to.
    //
    auto local_commit_pos = this->shared_state_.commit_pos.get_value();
    auto local_flush_pos = this->shared_state_.flush_pos.get_value();
    for (;;) {
      ConstBuffer bytes_to_flush{this->buffer_.get(local_flush_pos).data(),
                                 slot_distance(local_flush_pos, local_commit_pos)};

      if (bytes_to_flush.size() == 0) {
        // We've caught up!  Put this task to sleep awaiting more data to flush.
        //
        StatusOr<slot_offset_type> updated_commit_pos =
            await_slot_offset(local_flush_pos + 1, this->shared_state_.commit_pos);
        BATT_REQUIRE_OK(updated_commit_pos);
        local_commit_pos = *updated_commit_pos;
        continue;
      }

      auto append_status = this->active_file_.append(bytes_to_flush);
      BATT_REQUIRE_OK(append_status);

      // This task is the only updater of `flush_pos`, so just update our local value and push it
      // out to the Watch.
      //
      local_flush_pos += bytes_to_flush.size();
      this->shared_state_.flush_pos.set_value(local_flush_pos);

      // Check to see if the active file is big enough to be split.
      //
      if (this->active_file_.size() >= this->shared_state_.config.min_segment_split_size) {
        StatusOr<SegmentFile> next_segment = this->active_file_.split();
        BATT_REQUIRE_OK(next_segment);
        this->shared_state_.segments.push(std::move(*next_segment));
      }
    }
  }();

  LLFS_LOG_INFO() << "[FileLogDriver::flush_task_main] finished with status=" << status;
}

}  // namespace llfs
