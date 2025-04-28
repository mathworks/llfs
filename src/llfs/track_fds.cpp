//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/track_fds.hpp>
//

#include <llfs/logging.hpp>

#include <batteries/env.hpp>
#include <batteries/suppress.hpp>

#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_map>

#ifdef __linux__
//
#include <dirent.h>
#include <sys/types.h>
//
#endif

namespace llfs {

namespace {

const char* kVarName = "LLFS_TRACK_FDS";

struct TrackFdState {
  std::atomic<bool> enabled{batt::getenv_as<int>(kVarName).value_or(0) != 0};
  std::mutex mutex;
  std::unordered_map<int, std::unique_ptr<boost::stacktrace::stacktrace>> traces;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static TrackFdState& instance() noexcept
  {
    // Intentionally leaked, since if this is being deleted the process is going away anyhow, the
    // destructor of std::unordered_map in this case doesn't have any side-effects external to the
    // process, so it's better to keep the process tear-down as simple as possible.
    //
    static TrackFdState* const instance_ = new TrackFdState{};

    return *instance_;
  }
};

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::set<int>> get_open_fds()
{
  std::set<int> result;
  const char* dirpath = "/proc/self/fd";

  //+++++++++++-+-+--+----- --- -- -  -  -   -
#ifdef __linux__
  // TODO [tastolfi 2024-07-01] Make Windows compatible.
  //
  DIR* p_dir = opendir(dirpath);
  if (!p_dir) {
    return batt::status_from_errno(errno);
  }

  if (!p_dir) {
    LLFS_LOG_ERROR() << "Could not open " << dirpath;
  } else {
    auto on_scope_exit = batt::finally([&] {
      closedir(p_dir);
    });

    const int p_dir_fd = dirfd(p_dir);

    for (;;) {
      errno = 0;
      struct dirent* p_entry = readdir(p_dir);

      // If p_entry is null, we are done!
      //
      if (!p_entry) {
        if (errno != 0) {
          return batt::status_from_errno(errno);
        }
        break;
      }

      // If the entry name parses as an integer *and* it is not the open directory we are scanning,
      // then add it to the result set.
      //
      std::optional<int> fd = batt::from_string<int>(p_entry->d_name);
      if (fd && *fd != p_dir_fd) {
        result.emplace(*fd);
      }
    }
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -
#else
  std::error_code ec;
  std::filesystem::directory_iterator iter{dirpath, ec};
  if (ec) {
    LLFS_VLOG(1) << "failed to open '/proc/self/fd': " << ec.value() << ":" << ec.message();
    return {};
  }
  LLFS_VLOG(1) << "open succeeded; gathering fds";

  for (const std::filesystem::directory_entry& entry : iter) {
    LLFS_VLOG(1) << BATT_INSPECT(entry.path().stem());
    std::optional<int> fd = batt::from_string<int>(entry.path().stem().string());
    if (!fd) {
      continue;
    }
    result.emplace(*fd);
  }
#endif
  //+++++++++++-+-+--+----- --- -- -  -  -   -

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
int maybe_track_fd(int fd)
{
  if (fd >= 0 && is_fd_tracking_enabled()) {
    set_trace_for_fd(fd, boost::stacktrace::stacktrace{});
  }
  return fd;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool is_fd_tracking_enabled()
{
  return TrackFdState::instance().enabled.load();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool set_fd_tracking_enabled(bool on)
{
  return TrackFdState::instance().enabled.exchange(on);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<boost::stacktrace::stacktrace> get_trace_for_fd(int fd)
{
  TrackFdState& state = TrackFdState::instance();
  std::unique_lock<std::mutex> lock{state.mutex};
  auto iter = state.traces.find(fd);
  if (iter == state.traces.end() || iter->second == nullptr) {
    return None;
  }
  return *iter->second;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void set_trace_for_fd(int fd, Optional<boost::stacktrace::stacktrace> trace)
{
  TrackFdState& state = TrackFdState::instance();
  std::unique_lock<std::mutex> lock{state.mutex};
  if (!trace) {
    state.traces.erase(fd);
  } else {
    state.traces[fd] = std::make_unique<boost::stacktrace::stacktrace>(*trace);
  }
}

}  //namespace llfs
