//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/filesystem_page_device.hpp>
//

#include <llfs/logging.hpp>
#include <llfs/status_code.hpp>

namespace llfs {

std::string FilesystemPageDevice::filename_from_id(PageId id)
{
  std::ostringstream oss;
  oss << std::hex << std::setw(16) << std::setfill('0') << id.int_value();
  return std::move(oss).str();
}

std::unique_ptr<FilesystemPageDevice> FilesystemPageDevice::erase(
    const fs::path& parent_dir, page_device_id_int device_id, PageCount capacity,
    PageSize page_size, ConfirmThisWillEraseAllMyData confirm)
{
  if (confirm == ConfirmThisWillEraseAllMyData::kNo) {
    return nullptr;
  }

  // Remove the contents of the parent directory.
  //
  for (auto& p : fs::directory_iterator(parent_dir)) {
    LLFS_LOG_INFO() << "Removing: " << p.path();
    std::error_code ec;
    fs::remove_all(p.path(), ec);
    if (ec) {
      LLFS_LOG_ERROR() << "  Failed to remove " << p.path() << ": " << ec;
      return nullptr;
    }
  }

  // Write the config file.
  //
  {
    std::ofstream ofs(parent_dir / ".llfs");
    ofs << capacity << " " << device_id << " " << page_size;
    if (ofs.bad()) {
      return nullptr;
    }
  }

  return std::unique_ptr<FilesystemPageDevice>(
      new FilesystemPageDevice(page_size, batt::make_copy(parent_dir), device_id, capacity));
}

std::unique_ptr<FilesystemPageDevice> FilesystemPageDevice::open(const fs::path& parent_dir)
{
  page_id_int capacity;
  page_device_id_int device_id;
  u32 page_size;

  LLFS_LOG_INFO() << "opening FilesystemPageDevice at " << parent_dir;
  {
    std::ifstream ifs(parent_dir / ".llfs");
    ifs >> capacity >> device_id >> page_size;
    if (ifs.bad()) {
      LLFS_PLOG_ERROR() << "page device config could not be read: " << (parent_dir / ".llfs");
      return nullptr;
    }
  }
  return std::unique_ptr<FilesystemPageDevice>(new FilesystemPageDevice(
      PageSize{page_size}, batt::make_copy(parent_dir), device_id, PageCount{capacity}));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> FilesystemPageDevice::prepare(PageId page_id)
{
  metrics().prepare_count++;

  return PageBuffer::allocate(this->page_size_, page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FilesystemPageDevice::write(std::shared_ptr<const PageBuffer>&& page_buffer,
                                 WriteHandler&& handler)
{
  auto result = [&]() -> Status {
    metrics().commit_count++;

    BATT_CHECK_EQ(page_buffer->size(), this->page_size_);
    BATT_CHECK_EQ(get_page_size(page_buffer), this->page_size_);

    std::unique_lock<std::mutex> lock{this->mutex_};

    auto iter = pre_dropped_.find(page_buffer->page_id());
    if (iter != pre_dropped_.end()) {
      metrics().commit_drop_count++;
      pre_dropped_.erase(iter);
      return OkStatus();
    }

    std::ofstream ofs(page_file_from_id(page_buffer->page_id()),
                      std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);

    const ConstBuffer buf = get_const_buffer(page_buffer);
    if (!ofs.write((const char*)buf.data(), buf.size()).good()) {
      return make_status(StatusCode::kFilesystemPageWriteFailed);
    }

    auto add_to_live_set = batt::finally([&] {
      const bool was_inserted = this->live_.emplace(page_buffer->page_id()).second;
      BATT_CHECK(was_inserted);
    });

    return OkStatus();
  }();

  handler(result);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FilesystemPageDevice::read(PageId id, ReadHandler&& handler)
{
  auto result = [&]() -> StatusOr<std::shared_ptr<const PageBuffer>> {
    metrics().read_count++;

    // TODO [tastolfi 2020-12-07] Pool these?
    //
    std::shared_ptr<PageBuffer> page = PageBuffer::allocate(this->page_size_, id);

    BATT_CHECK_NOT_NULLPTR(page);
    BATT_CHECK_EQ(page->size(), this->page_size_);
    BATT_CHECK_EQ(get_page_size(page), this->page_size_);

    std::ifstream ifs(page_file_from_id(id));
    if (!ifs.good()) {
      LLFS_PLOG_ERROR() << "read of page: " << page_file_from_id(id) << " failed";
      return make_status(StatusCode::kFilesystemPageOpenFailed);
    }

    if (!ifs.read(reinterpret_cast<char*>(page.get()), page->size()).good()) {
      return make_status(StatusCode::kFilesystemPageReadFailed);
    }

    return {std::move(page)};
  }();

  handler(result);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void FilesystemPageDevice::drop(PageId id, WriteHandler&& handler)
{
  auto result = [&]() -> Status {
    metrics().drop_count++;

    std::unique_lock<std::mutex> lock{this->mutex_};

    std::error_code ec;
    fs::path page_file = page_file_from_id(id);

    if (!fs::remove(page_file, ec)) {
      LLFS_DLOG_WARNING() << "failed to delete file: " << page_file
                          << " (is_live=" << this->live_.count(id) << ")";
      metrics().drop_false_count++;
      pre_dropped_.emplace(id);
    } else if (ec) {
      metrics().drop_error_count++;
      LLFS_DLOG_WARNING() << "drop page error: value=" << ec.value() << " message='" << ec.message()
                          << "'";
      return make_status(StatusCode::kFilesystemRemoveFailed);
    }
    return OkStatus();
  }();

  handler(result);
}

}  // namespace llfs
