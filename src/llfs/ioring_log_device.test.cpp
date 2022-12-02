//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_log_device.hpp>
//
#include <llfs/ioring_log_device.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/ioring.hpp>
#include <llfs/log_device_config.hpp>
#include <llfs/storage_context.hpp>
#include <llfs/uuid.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/static_assert.hpp>

#include <filesystem>

namespace {

using namespace batt::int_types;
using namespace batt::constants;

auto constexpr kLogDeviceFileName = "/tmp/llfs_IoringLogDeviceTest_StorageFile.llfs";

constexpr usize kLogTotalSize = 16 * kKiB;
constexpr usize kLogBlockSize = 1 * kKiB;
constexpr usize kLogBlockCapacity = 960;
constexpr usize kLogPagesPerBlockLog2 = 1;

BATT_STATIC_ASSERT_EQ(kLogBlockSize, (usize{1} << kLogPagesPerBlockLog2) * llfs::kLogPageSize);

TEST(IoringLogDeviceTest, StorageFile)
{
  const std::filesystem::path kLogDeviceFilePath = kLogDeviceFileName;

  boost::uuids::uuid test_log_uuid = llfs::random_uuid();

  auto scoped_ioring =
      llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{256}, llfs::ThreadPoolSize{1});

  ASSERT_TRUE(scoped_ioring.ok()) << BATT_INSPECT(scoped_ioring.status());

  auto storage_context = batt::make_shared<llfs::StorageContext>(
      batt::Runtime::instance().default_scheduler(), scoped_ioring->get());

  // Remove the file if it already exists.
  //
  std::filesystem::remove_all(kLogDeviceFilePath);
  ASSERT_TRUE(!std::filesystem::exists(kLogDeviceFilePath));

  // Create a log device file.
  //
  batt::Status add_file_status = storage_context->add_new_file(
      kLogDeviceFilePath, [&](llfs::StorageFileBuilder& builder) -> batt::Status {
        BATT_REQUIRE_OK(builder.add_object(llfs::LogDeviceConfigOptions{
            .uuid = test_log_uuid,
            .pages_per_block_log2 = kLogPagesPerBlockLog2,
            .log_size = kLogTotalSize,
        }));

        return batt::OkStatus();
      });

  ASSERT_TRUE(add_file_status.ok()) << BATT_INSPECT(add_file_status);

  {
    // Recover the log config from the file.
    //
    batt::StatusOr<std::unique_ptr<llfs::IoRingLogDeviceFactory>> log_device_factory =
        storage_context->recover_object(batt::StaticType<llfs::PackedLogDeviceConfig>{},
                                        test_log_uuid,
                                        llfs::IoRingLogDriverOptions::with_default_values()
                                            .set_name("test_log")
                                            .set_queue_depth(2));

    ASSERT_TRUE(log_device_factory.ok()) << BATT_INSPECT(log_device_factory.status());

    {
      // Open the log using the recovered config.
      //
      batt::StatusOr<std::unique_ptr<llfs::IoRingLogDevice>> status_or_log_device =
          (**log_device_factory).open_ioring_log_device();
      ASSERT_TRUE(status_or_log_device.ok()) << BATT_INSPECT(status_or_log_device.status());

      llfs::IoRingLogDevice& log_device = **status_or_log_device;

      llfs::IoRingLogDriver& driver = log_device.driver().impl();
      EXPECT_EQ(driver.calculate().block_capacity(), kLogBlockCapacity);

      // Write exactly one block's worth of data.
      //
      llfs::LogDevice::Writer& writer = log_device.writer();

      batt::StatusOr<llfs::MutableBuffer> dst_buffer = writer.prepare(kLogBlockCapacity);
      ASSERT_TRUE(dst_buffer.ok()) << BATT_INSPECT(dst_buffer.status());
      ASSERT_GE(dst_buffer->size(), kLogBlockCapacity);

      std::memset(dst_buffer->data(), 'a', kLogBlockCapacity);

      batt::StatusOr<llfs::slot_offset_type> commit_status = writer.commit(kLogBlockCapacity);
      ASSERT_TRUE(commit_status.ok()) << BATT_INSPECT(commit_status);

      batt::Status sync_status =
          log_device.sync(llfs::LogReadMode::kDurable, llfs::SlotUpperBoundAt{kLogBlockCapacity});
      ASSERT_TRUE(sync_status.ok()) << BATT_INSPECT(sync_status);

      // Close the device.
      //
      batt::Status close_status = log_device.close();
      ASSERT_TRUE(close_status.ok()) << BATT_INSPECT(close_status);
    }
  }

  // Re-open the log.
  //
  {
    // Recover the log config from the file.
    //
    batt::StatusOr<std::unique_ptr<llfs::IoRingLogDeviceFactory>> log_device_factory =
        storage_context->recover_object(batt::StaticType<llfs::PackedLogDeviceConfig>{},
                                        test_log_uuid,
                                        llfs::IoRingLogDriverOptions::with_default_values()
                                            .set_name("test_log")
                                            .set_queue_depth(2));

    ASSERT_TRUE(log_device_factory.ok()) << BATT_INSPECT(log_device_factory.status());

    {
      // Open the log using the recovered config.
      //
      batt::StatusOr<std::unique_ptr<llfs::IoRingLogDevice>> status_or_log_device =
          (**log_device_factory).open_ioring_log_device();

      ASSERT_TRUE(status_or_log_device.ok()) << BATT_INSPECT(status_or_log_device.status());
    }
  }
}

}  // namespace
