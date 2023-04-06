//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/storage_simulation.hpp>
//

#include <llfs/simulated_log_device.hpp>
#include <llfs/simulated_log_device_impl.hpp>
#include <llfs/simulated_page_device.hpp>
#include <llfs/simulated_page_device_impl.hpp>

namespace llfs {

class StorageSimulation::TaskSchedulerImpl : public batt::TaskScheduler
{
 public:
  explicit TaskSchedulerImpl(StorageSimulation& simulation) noexcept : simulation_{simulation}
  {
  }

  boost::asio::any_io_executor schedule_task() override
  {
    return this->simulation_.fake_executor_;
  }

  void halt() override
  {
    //TODO [tastolfi 2023-04-05]
  }

  void join() override
  {
    //TODO [tastolfi 2023-04-05]
  }

 private:
  StorageSimulation& simulation_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ StorageSimulation::StorageSimulation(
    batt::StateMachineEntropySource&& entropy_source) noexcept
    : entropy_source_{std::move(entropy_source)}
    , task_scheduler_impl_{std::make_unique<TaskSchedulerImpl>(*this)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
batt::TaskScheduler& StorageSimulation::task_scheduler() noexcept
{
  return *this->task_scheduler_impl_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<LogDevice> StorageSimulation::get_log_device(const std::string& name,
                                                             Optional<u64> capacity)
{
  auto iter = this->log_devices_.find(name);

  // If we didn't find the named device, then create it.
  //
  if (iter == this->log_devices_.end()) {
    BATT_CHECK(capacity)
        << "Must specify capacity if creating a simulated log device for the first time!";

    iter = this->log_devices_
               .emplace(name, std::make_shared<SimulatedLogDevice::Impl>(*this, *capacity))
               .first;
  }

  // At this point we should have a valid entry.
  //
  BATT_CHECK_NE(iter, this->log_devices_.end());
  BATT_CHECK_NOT_NULLPTR(iter->second);

  return std::make_unique<SimulatedLogDevice>(batt::make_copy(iter->second));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<PageDevice> StorageSimulation::get_page_device(const std::string& name,
                                                               Optional<PageCount> page_count,
                                                               Optional<PageSize> page_size)
{
  auto iter = this->page_devices_.find(name);

  // If we didn't find the named device, then create it.
  //
  if (iter == this->page_devices_.end()) {
    BATT_CHECK(page_count && page_size)
        << "Must specify page count/size if creating a simulated page device for the first time!";

    const page_device_id_int next_page_device_id = this->page_devices_.size();

    iter = this->page_devices_
               .emplace(name, std::make_shared<SimulatedPageDevice::Impl>(
                                  *this, *page_size, *page_count, next_page_device_id))
               .first;
  }

  // At this point we should have a valid entry.
  //
  BATT_CHECK_NE(iter, this->page_devices_.end());
  BATT_CHECK_NOT_NULLPTR(iter->second);

  return std::make_unique<SimulatedPageDevice>(batt::make_copy(iter->second));
}

}  //namespace llfs
