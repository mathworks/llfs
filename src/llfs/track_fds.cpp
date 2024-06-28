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
    // Intentionally leaked.
    //
    static TrackFdState* const instance_ = new TrackFdState{};

    return *instance_;
  }
};

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::set<int> get_open_fds()
{
  std::set<int> result;
  const char* dirpath = "/proc/self/fd";

  //+++++++++++-+-+--+----- --- -- -  -  -   -
#ifdef __linux__
  DIR* p_dir = opendir(dirpath);

  if (!p_dir) {
    LLFS_LOG_ERROR() << "Could not open " << dirpath;
  } else {
    auto on_scope_exit = batt::finally([&] {
      closedir(p_dir);
    });

    const int p_dir_fd = dirfd(p_dir);

    isize name_max = pathconf(dirpath, _PC_NAME_MAX);
    if (name_max == -1) { /* Limit not defined, or error */
      name_max = 255;     /* Take a guess */
    }
    const usize len = offsetof(struct dirent, d_name) + name_max + 1;
    void* const entry_buffer = malloc(len);
    auto on_scope_exit2 = batt::finally([&] {
      free(entry_buffer);
    });

    for (;;) {
      struct dirent* p_entry = nullptr;

      BATT_SUPPRESS_IF_GCC("-Wdeprecated-declarations")
      const int retval = readdir_r(p_dir, (struct dirent*)entry_buffer, &p_entry);
      BATT_UNSUPPRESS_IF_GCC()

      // Check for errors.
      //
      if (retval != 0) {
        LLFS_LOG_ERROR() << "readdir_r returned: " << retval;
        break;
      }

      // If p_entry is null, we are done!
      //
      if (!p_entry) {
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
