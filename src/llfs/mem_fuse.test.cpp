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
#include <iostream>
#include <thread>

namespace {

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

TEST(MemFuseTest, Test)
{
  google::InitGoogleLogging("llfs_Test", google::CustomPrefixCallback{CustomPrefix},
                            /*prefix_callback_data=*/nullptr);

  LLFS_LOG_INFO() << "Test log message";

  struct stat st;
  std::memset(&st, 0, sizeof(st));

  batt::enable_dump_tasks();

  int rt = lstat(".", &st);

  std::cout << std::endl << llfs::DumpStat{st} << BATT_INSPECT(rt) << std::endl << std::endl;

  auto work_queue = std::make_shared<llfs::WorkQueue>();

  std::thread t{[&work_queue] {
    boost::asio::io_context io;

    llfs::WorkerTask task{batt::make_copy(work_queue), io.get_executor()};

    io.run();
  }};

  t.detach();

  auto mountpoint = std::filesystem::path{"/tmp/llfs_fuse_test"};
  std::string mountpoint_str = mountpoint.string();

  if (std::filesystem::exists(mountpoint)) {
    std::filesystem::remove_all(mountpoint);
  }
  std::filesystem::create_directories(mountpoint);

  const char* argv[] = {
      "llfs_Test",
      mountpoint_str.c_str(),
  };
  int argc = sizeof(argv) / sizeof(const char*);

  LLFS_LOG_INFO() << BATT_INSPECT(argc);

  batt::StatusOr<llfs::FuseSession> session = llfs::FuseSession::from_args(
      argc, argv, batt::StaticType<llfs::MemoryFuseImpl>{}, batt::make_copy(work_queue));

  BATT_CHECK_OK(session);

  std::thread session_thread{[&] {
    session->run();
  }};

  session->halt();

  session_thread.join();
}

}  // namespace
