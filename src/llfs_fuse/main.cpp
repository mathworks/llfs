//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/fuse.hpp>
#include <llfs/mem_fuse.hpp>
#include <llfs/worker_task.hpp>

#include <batteries/async/dump_tasks.hpp>

#include <glog/logging.h>

#include <boost/asio/io_context.hpp>

#include <iostream>
#include <thread>

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

struct CustomLogSink : google::LogSink {
  void send(google::LogSeverity severity, const char* full_filename, const char* base_filename,
            int line, const google::LogMessageTime& time, const char* message,
            std::size_t message_len) override
  {
    auto& s = std::cerr;

    switch (severity) {
      case google::GLOG_ERROR:
        s << termxx::color::Red;
        break;

      case google::GLOG_WARNING:
        s << termxx::color::Yellow;
        break;

      case google::GLOG_INFO:
        s << termxx::color::Blue;
        break;

      case google::GLOG_FATAL:  // fall-through
      default:
        break;
    }

    s << google::GetLogSeverityName(severity)
      << " ["  //

      //----- --- -- -  -  -   -
      << termxx::color::Green                     //
      << std::setw(4) << 1900 + time.year()       //
      << "/" << std::setw(2) << 1 + time.month()  //
      << "/" << std::setw(2)
      << time.day()  //

      //----- --- -- -  -  -   -
      << termxx::color::White << termxx::style::Bold  //
      << ' ' << std::setw(2) << time.hour()           //
      << ':' << std::setw(2) << time.min()            //
      << ':' << std::setw(2) << time.sec()            //
      << "." << std::setw(6) << time.usec()           //
      << termxx::Reset                                //

      //----- --- -- -  -  -   -
      << termxx::color::Cyan  //
      << ' ' << std::setfill(' ') << std::setw(5) << std::this_thread::get_id() << std::setfill('0')
      << ' '

      //----- --- -- -  -  -   -
      << termxx::color::Blue << termxx::style::Underline  //
      << base_filename << ':' << line
      << "]"  //

      //----- --- -- -  -  -   -
      << termxx::Reset;
  }
};

int main(int argc, char* argv[])
{
  google::InitGoogleLogging(argv[0]);

  CustomLogSink sink;
  google::AddLogSink(&sink);

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

  batt::StatusOr<llfs::FuseSession> session = llfs::FuseSession::from_args(
      argc, (const char**)argv, batt::StaticType<llfs::MemoryFuseImpl>{},
      batt::make_copy(work_queue));

  BATT_CHECK_OK(session);

  return session->run();
}
