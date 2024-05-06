//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/simulated_log_device_storage.hpp>
//
#include <llfs/storage_simulation.hpp>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ SimulatedLogDeviceStorage::EphemeralState::EphemeralState(
    std::shared_ptr<DurableState>&& durable_state) noexcept
    : durable_state_{std::move(durable_state)}
    , simulation_{this->durable_state_->simulation()}
    , creation_step_{this->simulation_.current_step()}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::post_to_event_loop(
    std::function<void(StatusOr<i32>)>&& handler)
{
  this->work_count_.fetch_add(1);

  this->simulation_.post([this, handler = BATT_FORWARD(handler)]() mutable {
    Status push_status = this->queue_.push([this, handler]() mutable {
      BATT_FORWARD(handler)(StatusOr<i32>{0});
    });

    if (!push_status.ok()) {
      this->work_count_.fetch_sub(1);
      handler(push_status);
    }
  });
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::async_write_some_fixed(
    i64 file_offset, const ConstBuffer& data, i32 /*buf_index*/,
    std::function<void(StatusOr<i32>)>&& handler)
{
  if (this->closed_.load()) {
    BATT_FORWARD(handler)(Status{batt::StatusCode::kClosed});
    return;
  }

  auto* write_task = new batt::Task{
      this->simulation_.task_scheduler().schedule_task(),
      [this, file_offset, data, handler = BATT_FORWARD(handler)]() mutable {
        Status status =
            this->multi_block_iop<ConstBuffer, &DurableState::write_block>(file_offset, data);

        if (status.ok()) {
          BATT_FORWARD(handler)(StatusOr<i32>{static_cast<i32>(data.size())});
        } else {
          BATT_FORWARD(handler)(status);
        }
      },
      batt::Task::DeferStart{true}};

  write_task->call_when_done([write_task] {
    delete write_task;
  });

  write_task->start();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void SimulatedLogDeviceStorage::EphemeralState::simulation_post(std::function<void()>&& fn)
{
  this->simulation_.post(std::move(fn));
}

}  //namespace llfs
