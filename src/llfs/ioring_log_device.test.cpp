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

#ifndef LLFS_DISABLE_IO_URING

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/ioring.hpp>
#include <llfs/ioring_log_device.test.hpp>
#include <llfs/log_device_config.hpp>
#include <llfs/storage_context.hpp>
#include <llfs/uuid.hpp>

#include <batteries/async/runtime.hpp>
#include <batteries/env.hpp>
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
  for (usize amount_to_write :
       {usize{1}, kLogBlockCapacity / 2, kLogBlockCapacity, kLogTotalSize}) {
    const std::filesystem::path kLogDeviceFilePath = kLogDeviceFileName;

    boost::uuids::uuid test_log_uuid = llfs::random_uuid();

    auto scoped_ioring =
        llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{256}, llfs::ThreadPoolSize{1});

    ASSERT_TRUE(scoped_ioring.ok()) << BATT_INSPECT(scoped_ioring.status());

    auto storage_context = batt::make_shared<llfs::StorageContext>(
        batt::Runtime::instance().default_scheduler(), scoped_ioring->get_io_ring());

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

        // Append some data.
        //
        llfs::LogDevice::Writer& writer = log_device.writer();

        batt::StatusOr<llfs::MutableBuffer> dst_buffer = writer.prepare(amount_to_write);
        ASSERT_TRUE(dst_buffer.ok()) << BATT_INSPECT(dst_buffer.status());
        ASSERT_GE(dst_buffer->size(), amount_to_write);

        std::memset(dst_buffer->data(), 'a', amount_to_write);

        batt::StatusOr<llfs::slot_offset_type> commit_status = writer.commit(amount_to_write);
        ASSERT_TRUE(commit_status.ok()) << BATT_INSPECT(commit_status);

        batt::Status sync_status =
            log_device.sync(llfs::LogReadMode::kDurable, llfs::SlotUpperBoundAt{amount_to_write});
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
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/** \brief Runs a LogDevice flush throughput microbenchmark.
 *
 * This is normally disabled; to enable it, define the env var LLFS_LOG_DEVICE_FILE to specify where
 * to create the llfs storage file that will contain the log device.
 *
 * The benchmark behavior is configured via env vars.  The basic workload is to create the storage
 * file with a log device of the specified size (LLFS_LOG_DEVICE_SIZE_KB) and pages per block
 * (LLFS_LOG_DEVICE_PAGES_PER_BLOCK), then launch two threads, an appender and a trimmer.
 *
 * The appender thread will write records of size LLFS_LOG_DEVICE_APPEND_SIZE bytes until it has
 * successfully appended a total of LLFS_LOG_DEVICE_WRITE_KB kilobytes worth of data.  Then the
 * appender waits for flush to complete, halts the LogDevice, and exits.
 *
 * The trimmer thread waits until the amount of active data in the log is at least
 * LLFS_LOG_DEVICE_TRIM_TRIGGER bytes, then trims exactly LLFS_LOG_DEVICE_TRIM_SIZE bytes, looping
 * until an error is encountered.  Under normal circumstances, this will happen when the appender
 * thread has finished and has halted the LogDevice.
 */
TEST(IoringLogDeviceTest, Benchmark)
{
  using llfs::read_test_var;

  const char* file_name =  //
      std::getenv("LLFS_LOG_DEVICE_FILE");

  if (!file_name) {
    LLFS_LOG_INFO() << "LLFS_LOG_DEVICE_FILE not specified; skipping benchmark test";
    return;
  }

  std::cout << "LLFS_LOG_DEVICE_FILE=" << batt::c_str_literal(file_name) << std::endl;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  const usize queue_depth = read_test_var("LLFS_STORAGE_CONTEXT_QUEUE_DEPTH", usize{64});
  const usize thread_pool_size = read_test_var("LLFS_STORAGE_CONTEXT_THREADS", usize{1});
  const usize pages_per_block = read_test_var("LLFS_LOG_DEVICE_PAGES_PER_BLOCK", usize{32});
  const usize log_queue_depth = read_test_var("LLFS_LOG_DEVICE_QUEUE_DEPTH", usize{1024});

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  llfs::run_log_device_benchmark([&](usize log_size, bool create, auto&& consume_log_fn) {
    BATT_CHECK(create);

    auto scoped_ioring = llfs::ScopedIoRing::make_new(llfs::MaxQueueDepth{queue_depth},
                                                      llfs::ThreadPoolSize{thread_pool_size});

    ASSERT_TRUE(scoped_ioring.ok()) << BATT_INSPECT(scoped_ioring.status());

    auto storage_context = batt::make_shared<llfs::StorageContext>(
        batt::Runtime::instance().default_scheduler(), scoped_ioring->get_io_ring());

    // Remove the file if it already exists.
    //
    std::filesystem::path file_path{file_name};
    std::filesystem::remove_all(file_path);
    ASSERT_FALSE(std::filesystem::exists(file_path));

    // Create a log device file.
    //
    const boost::uuids::uuid test_log_uuid = llfs::random_uuid();

    batt::Status add_file_status = storage_context->add_new_file(
        file_path, [&](llfs::StorageFileBuilder& builder) -> batt::Status {
          BATT_REQUIRE_OK(builder.add_object(llfs::LogDeviceConfigOptions{
              .uuid = test_log_uuid,
              .pages_per_block_log2 = batt::log2_ceil(pages_per_block),
              .log_size = log_size,
          }));

          return batt::OkStatus();
        });

    ASSERT_TRUE(add_file_status.ok()) << BATT_INSPECT(add_file_status);

    // Recover the log config from the file.
    //
    batt::StatusOr<std::unique_ptr<llfs::IoRingLogDeviceFactory>> log_device_factory =
        storage_context->recover_object(batt::StaticType<llfs::PackedLogDeviceConfig>{},
                                        test_log_uuid,
                                        llfs::IoRingLogDriverOptions::with_default_values()
                                            .set_name("test_log")
                                            .set_queue_depth(log_queue_depth));

    // Open the log using the recovered config.
    //
    batt::StatusOr<std::unique_ptr<llfs::IoRingLogDevice>> status_or_log_device =
        (**log_device_factory).open_ioring_log_device();

    ASSERT_TRUE(status_or_log_device.ok()) << BATT_INSPECT(status_or_log_device.status());

    llfs::IoRingLogDevice& log_device = **status_or_log_device;

    // Run the workload.
    //
    consume_log_fn(log_device);
  });
}

}  // namespace

#endif  // LLFS_DISABLE_IO_URING
