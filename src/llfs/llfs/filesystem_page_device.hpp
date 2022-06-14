#pragma once
#ifndef LLFS_FILESYSTEM_PAGE_DEVICE_HPP
#define LLFS_FILESYSTEM_PAGE_DEVICE_HPP

#include <llfs/confirm.hpp>
#include <llfs/metrics.hpp>
#include <llfs/optional.hpp>
#include <llfs/page_device.hpp>

#include <glog/logging.h>

#include <batteries/assert.hpp>

#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
#include <unordered_set>

namespace llfs {

namespace fs = std::filesystem;

// Reference implementation of PageDevice.  This is slow; not to be used in
// production... for testing/development only!
//
class FilesystemPageDevice : public PageDevice
{
 public:
  struct Metrics {
    CountMetric<u64> prepare_count{0};
    CountMetric<u64> commit_count{0};
    CountMetric<u64> commit_drop_count{0};
    CountMetric<u64> read_count{0};
    CountMetric<u64> drop_count{0};
    CountMetric<u64> drop_false_count{0};
    CountMetric<u64> drop_error_count{0};
  };

  static Metrics& metrics()
  {
    static Metrics m_;
    return m_;
  }

  static std::string filename_from_id(PageId id);

  static std::unique_ptr<FilesystemPageDevice> open(const fs::path& parent_dir);

  static std::unique_ptr<FilesystemPageDevice> erase(const fs::path& parent_dir,
                                                     page_device_id_int device_id,
                                                     page_id_int capacity, u32 page_size,
                                                     ConfirmThisWillEraseAllMyData confirm);

  u32 page_size() override
  {
    return this->page_size_;
  }

  PageIdFactory page_ids() override
  {
    return PageIdFactory{this->capacity_, this->device_id_};
  }

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  StatusOr<std::shared_ptr<PageBuffer>> prepare(PageId id) override;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void write(std::shared_ptr<const PageBuffer>&& page_buffer, WriteHandler&& handler) override;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void read(PageId id, ReadHandler&& handler) override;

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  void drop(PageId id, WriteHandler&& handler) override;

 private:
  explicit FilesystemPageDevice(u32 page_size, fs::path&& parent_dir, page_device_id_int device_id,
                                i64 capacity) noexcept
      : page_size_{page_size}
      , parent_dir_(std::move(parent_dir))
      , device_id_{device_id}
      , capacity_{capacity}
  {
  }

  fs::path page_file_from_id(PageId id) const
  {
    return parent_dir_ / filename_from_id(id);
  }

  const u32 page_size_;
  const fs::path parent_dir_;
  const page_device_id_int device_id_;
  const i64 capacity_;
  std::mutex mutex_;
  std::unordered_set<PageId, PageId::Hash> pre_dropped_;
  std::unordered_set<PageId, PageId::Hash> live_;
};

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

inline std::ostream& operator<<(std::ostream& out, const FilesystemPageDevice::Metrics& t)
{
  return out << "FSPageDevice::Metrics{.prepares=" << t.prepare_count
             << ", .commits(total)=" << t.commit_count << ", .commits(drop)=" << t.commit_drop_count
             << ", .reads=" << t.read_count << ", .drops(total)=" << t.drop_count
             << ", .drops(false)=" << t.drop_false_count << ", .drops(error)=" << t.drop_error_count
             << ",}";
}

}  // namespace llfs

#endif  // LLFS_FILESYSTEM_PAGE_DEVICE_HPP
