//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/mem_fuse.hpp>
//
#include <llfs/mem_fuse.hpp>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <llfs/fuse.hpp>
#include <llfs/worker_task.hpp>

#include <batteries/async/dump_tasks.hpp>

#include <glog/logging.h>

#include <boost/asio/io_context.hpp>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>

#include <sys/mount.h>

namespace {

using namespace llfs::int_types;

namespace termxx {

namespace color {

constexpr auto Black = "\033[30m";
constexpr auto Red = "\033[31m";
constexpr auto Green = "\033[32m";
constexpr auto Yellow = "\033[33m";
constexpr auto Blue = "\033[34m";
constexpr auto Magenta = "\033[35m";
constexpr auto Cyan = "\033[36m";
constexpr auto White = "\033[37m";

}  //namespace color

namespace fill {

constexpr auto Black = "\033[40m";
constexpr auto Red = "\033[41m";
constexpr auto Green = "\033[42m";
constexpr auto Yellow = "\033[43m";
constexpr auto Blue = "\033[44m";
constexpr auto Magenta = "\033[45m";
constexpr auto Cyan = "\033[46m";
constexpr auto White = "\033[47m";

}  //namespace fill

namespace style {

constexpr auto Bold = "\033[1m";
constexpr auto BoldOff = "\033[21m";

constexpr auto Italic = "\033[3m";
constexpr auto ItalicOff = "\033[23m";

constexpr auto Underline = "\033[4m";
constexpr auto UnderlineOff = "\033[24m";

constexpr auto Inverse = "\033[7m";
constexpr auto InverseOff = "\033[27m";

}  //namespace style

constexpr auto Reset = "\033[0m";

}  //namespace termxx

void CustomPrefix(std::ostream& s, const google::LogMessageInfo& l, void*)
{
  switch (l.severity[0]) {
    case 'E':
      s << termxx::color::Red;
      break;

    case 'W':
      s << termxx::color::Yellow;
      break;

    case 'I':
      s << termxx::color::Blue;
      break;
  }

  s << l.severity     //
    << termxx::Reset  //
    << " ["           //

    //----- --- -- -  -  -   -
    << termxx::style::Italic                      //
    << termxx::color::Magenta                     //
    << std::setw(4) << 1900 + l.time.year()       //
    << "/" << std::setw(2) << 1 + l.time.month()  //
    << "/" << std::setw(2) << l.time.day()        //
    << termxx::style::ItalicOff

    //----- --- -- -  -  -   -
    << termxx::color::White << termxx::style::Bold  //
    << ' ' << std::setw(2) << l.time.hour()         //
    << ':' << std::setw(2) << l.time.min()          //
    << ':' << std::setw(2) << l.time.sec()          //
    << "." << std::setw(6) << l.time.usec()         //
    << termxx::Reset                                //

    //----- --- -- -  -  -   -
    << termxx::color::Cyan  //
    << ' ' << std::setfill(' ') << std::setw(5) << l.thread_id << std::setfill('0')
    << ' '

    //----- --- -- -  -  -   -
    << termxx::color::Blue << termxx::style::Underline  //
    << l.filename << ':' << l.line_number               //
    << termxx::Reset                                    //
    << "]"                                              //

    //----- --- -- -  -  -   -
    << termxx::Reset;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class MemFuseTest : public ::testing::Test
{
 public:
  void SetUp() override
  {
    // Initialize and configure logging.
    //
    if (false) {
      google::InitGoogleLogging("llfs_Test", google::CustomPrefixCallback{CustomPrefix},
                                /*prefix_callback_data=*/nullptr);
    }

    // Enable task dumping signal handler.
    //
    batt::enable_dump_tasks();

    // Initialize the work queue used by MemFuseImpl.
    //
    this->work_queue_ = std::make_shared<llfs::WorkQueue>();

    // Start a single thread to pull work from the queue.
    //
    this->worker_task_thread_.emplace([this] {
      boost::asio::io_context io;

      llfs::WorkerTask task{batt::make_copy(this->work_queue_), io.get_executor()};

      io.run();
    });

    const auto try_umount = [&] {
      LLFS_LOG_INFO() << "Attempting to umount " << this->mountpoint_;
      int retval = umount2(this->mountpoint_str_.c_str(), MNT_FORCE);
      LLFS_LOG_INFO() << BATT_INSPECT(batt::status_from_retval(retval));
    };

    // Create a fresh mount point directory.
    //
    for (int retry = 0; retry < 2; ++retry) {
      std::error_code ec;
      bool mountpoint_exists = std::filesystem::exists(this->mountpoint_, ec);
      if (ec && retry == 0) {
        try_umount();
        continue;
      }

      if (mountpoint_exists) {
        std::filesystem::remove_all(this->mountpoint_, ec);
        ASSERT_FALSE(ec) << "Failed to remove mountpoint";
      }

      std::filesystem::create_directories(this->mountpoint_, ec);
      if (ec && retry == 0) {
        try_umount();
        continue;
      }
      ASSERT_FALSE(ec) << "Failed to initialize mountpoint:"
                       << BATT_INSPECT_STR(this->mountpoint_.string()) << BATT_INSPECT(ec.value())
                       << BATT_INSPECT(ec.message());
    }

    // Start FUSE session on a background thread.
    //
    {
      BATT_CHECK_NOT_NULLPTR(this->work_queue_);

      batt::StatusOr<llfs::FuseSession> status_or_session = llfs::FuseSession::from_args(
          this->argc_, this->argv_.data(), batt::StaticType<llfs::MemoryFuseImpl>{},
          batt::make_copy(this->work_queue_));

      ASSERT_TRUE(status_or_session.ok()) << BATT_INSPECT(status_or_session.status());

      BATT_CHECK_EQ(this->fuse_session_, batt::None);
      this->fuse_session_ = std::move(*status_or_session);
    }
    BATT_CHECK_EQ(this->fuse_session_thread_, batt::None);

    this->fuse_session_thread_.emplace([this] {
      BATT_CHECK_NE(this->fuse_session_, batt::None);
      this->fuse_session_->run();
    });
  }

  void TearDown() override
  {
    if (this->work_queue_) {
      LLFS_LOG_INFO() << "Closing work queue";
      this->work_queue_->close();
      if (this->worker_task_thread_) {
        LLFS_LOG_INFO() << "Joining worker task thread";
        this->worker_task_thread_->join();
        this->worker_task_thread_ = batt::None;
      }
    } else {
      BATT_CHECK_EQ(this->worker_task_thread_, batt::None);
    }

    if (this->fuse_session_) {
      LLFS_LOG_INFO() << "Halting fuse session";
      this->fuse_session_->halt();
      if (this->fuse_session_thread_) {
        LLFS_LOG_INFO() << "Joining fuse thread";
        this->fuse_session_thread_->join();
        this->fuse_session_thread_ = batt::None;
      }
    }

    this->work_queue_ = nullptr;
    this->fuse_session_ = batt::None;
  }

  void print_lstat()
  {
    struct stat st;
    std::memset(&st, 0, sizeof(st));

    int rt = lstat(".", &st);

    std::cout << std::endl << llfs::DumpStat{st} << BATT_INSPECT(rt) << std::endl << std::endl;
  }

  batt::StatusOr<std::vector<std::filesystem::directory_entry>> find_files()
  {
    std::vector<std::filesystem::directory_entry> files;

    std::error_code ec;
    for (const std::filesystem::directory_entry& entry :
         std::filesystem::recursive_directory_iterator(this->mountpoint_, ec)) {
      files.push_back(entry);
    }

    BATT_REQUIRE_OK(ec);

    std::sort(files.begin(), files.end(),
              [](const std::filesystem::directory_entry& left,
                 const std::filesystem::directory_entry& right) {
                return left.path().string() < right.path().string();
              });

    return files;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::shared_ptr<llfs::WorkQueue> work_queue_;

  batt::Optional<std::thread> worker_task_thread_;

  const std::filesystem::path mountpoint_{"/tmp/llfs_fuse_test"};

  const std::string mountpoint_str_ = this->mountpoint_.string();

  std::array<const char*, 2> argv_{
      "llfs_Test",
      this->mountpoint_str_.c_str(),
  };

  const int argc_ = this->argv_.size();

  batt::Optional<llfs::FuseSession> fuse_session_;

  batt::Optional<std::thread> fuse_session_thread_;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(MemFuseTest, StartStop)
{
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
TEST_F(MemFuseTest, CreateFile)
{
  const std::string_view data1 = "Some stuff.";
  const std::string data2 = [] {
    std::ifstream ifs{__FILE__};
    std::ostringstream oss;
    oss << ifs.rdbuf();
    return oss.str();
  }();

  ASSERT_GT(data2.size(), 4096u);

  // Initially there should be no files.
  {
    batt::StatusOr<std::vector<std::filesystem::directory_entry>> files = this->find_files();

    ASSERT_TRUE(files.ok()) << BATT_INSPECT(files.status());
    EXPECT_TRUE(files->empty());
  }

  // Create some files.
  {
    std::ofstream ofs{this->mountpoint_ / "file.txt"};
    ofs << data1;
  }
  {
    std::ofstream ofs{this->mountpoint_ / "file2.txt"};
    ofs << data2;
  }

  // Expect to find the file we created.
  {
    batt::StatusOr<std::vector<std::filesystem::directory_entry>> files = this->find_files();

    ASSERT_TRUE(files.ok()) << BATT_INSPECT(files.status());
    ASSERT_EQ(files->size(), 2u);

    EXPECT_TRUE((*files)[0].is_regular_file());
    EXPECT_EQ((*files)[0].path(), this->mountpoint_ / "file.txt");
    EXPECT_EQ((*files)[0].file_size(), data1.size());

    EXPECT_TRUE((*files)[1].is_regular_file());
    EXPECT_EQ((*files)[1].path(), this->mountpoint_ / "file2.txt");
    EXPECT_EQ((*files)[1].file_size(), data2.size());
  }

  // Read the file we created above.
  {
    std::ifstream ifs{this->mountpoint_ / "file.txt"};

    EXPECT_TRUE(ifs.good());

    std::ostringstream oss;
    oss << ifs.rdbuf();

    EXPECT_THAT(oss.str(), ::testing::StrEq(data1));
    EXPECT_FALSE(oss.bad());
    EXPECT_FALSE(ifs.bad());
  }

  // Truncate the other file and read it.
  {
    // Verify the original contents.
    {
      std::ifstream ifs{this->mountpoint_ / "file2.txt"};
      std::ostringstream oss;
      oss << ifs.rdbuf();

      EXPECT_EQ(oss.str().size(), data2.size());
      EXPECT_THAT(oss.str(), ::testing::StrEq(data2));
    }

    const auto resize_and_verify = [&](u64 newsize, u64 expect_from_data2, u64 expect_zeros) {
      BATT_CHECK_EQ(newsize, expect_from_data2 + expect_zeros)
          << BATT_INSPECT(expect_from_data2) << BATT_INSPECT(expect_zeros);

      std::error_code ec;
      std::filesystem::resize_file(this->mountpoint_ / "file2.txt", newsize, ec);

      EXPECT_FALSE(ec);
      EXPECT_EQ(std::filesystem::file_size(this->mountpoint_ / "file2.txt", ec), newsize);
      EXPECT_FALSE(ec);

      std::ifstream ifs{this->mountpoint_ / "file2.txt"};
      std::ostringstream oss;
      oss << ifs.rdbuf();

      EXPECT_EQ(oss.str().size(), newsize);
      EXPECT_THAT(oss.str(), ::testing::StrEq(data2.substr(0, expect_from_data2) +
                                              std::string(expect_zeros, '\0')));
    };

    resize_and_verify(5555, 5555, 0);
    resize_and_verify(5655, 5555, 100);
    resize_and_verify(4096, 4096, 0);
    resize_and_verify(4000, 4000, 0);
    resize_and_verify(3900, 3900, 0);
    resize_and_verify(3950, 3900, 50);
  }
}

}  // namespace
