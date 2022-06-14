#include <llfs/ioring_page_file_device.hpp>
//

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRingPageFileDevice::IoRingPageFileDevice(
    IoRing::File&& file, const FileOffsetPtr<PackedPageDeviceConfig>& config) noexcept
    : file_{std::move(file)}
    , config_{config}
    , page_ids_{this->config_->page_count, this->config_->device_id}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageIdFactory IoRingPageFileDevice::page_ids()
{
  return this->page_ids_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u32 IoRingPageFileDevice::page_size()
{
  return batt::checked_cast<u32>(this->config_->page_size());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> IoRingPageFileDevice::prepare(PageId page_id)
{
  StatusOr<u64> physical_page = this->get_physical_page(page_id);
  BATT_REQUIRE_OK(physical_page);

  return PageBuffer::allocate(this->page_size(), page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::write(std::shared_ptr<const PageBuffer>&& page_buffer,
                                 WriteHandler&& handler)
{
  StatusOr<i64> page_offset_in_file = this->get_file_offset_of_page(page_buffer->page_id());
  if (!page_offset_in_file.ok()) {
    handler(page_offset_in_file.status());
    return;
  }
  this->write_some(*page_offset_in_file, std::move(page_buffer), /*n_written=*/0,
                   std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::write_some(i64 page_offset_in_file,
                                      std::shared_ptr<const PageBuffer>&& page_buffer,
                                      usize n_written_so_far, WriteHandler&& handler)
{
  BATT_CHECK_GE(page_offset_in_file, 0);

  // TODO [tastolfi 2021-06-11] only write "live" data.
  //
  BATT_CHECK_LE(n_written_so_far, page_buffer->size());
  ConstBuffer data = page_buffer->const_buffer() + n_written_so_far;

  this->file_.async_write_some(
      page_offset_in_file + n_written_so_far, data,
      bind_handler(std::move(handler), [this, page_offset_in_file,
                                        page_buffer = std::move(page_buffer), n_written_so_far](
                                           WriteHandler&& handler, StatusOr<i32> result) mutable {
        if (!result.ok()) {
          if (batt::status_is_retryable(result.status())) {
            this->write_some(page_offset_in_file, std::move(page_buffer), n_written_so_far,
                             std::move(handler));
            return;
          }

          LOG(WARNING) << "IoRingPageFileDevice::write failed; page_offset_in_file+"
                       << n_written_so_far << "=" << page_offset_in_file + n_written_so_far
                       << " n_written_so_far=" << n_written_so_far
                       << " page_offset_in_file=" << page_offset_in_file;

          handler(result.status());
          return;
        }
        BATT_CHECK_GT(*result, 0) << "We must either make progress or receive an error code!";

        n_written_so_far += *result;

        // If we have reached the end of the data, invoke the handler.
        //
        if (n_written_so_far == page_buffer->size()) {
          handler(OkStatus());
          return;
        }

        // The write was short; write again from the new stop point.
        //
        this->write_some(page_offset_in_file, std::move(page_buffer), n_written_so_far,
                         std::move(handler));
      }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::read(PageId page_id, ReadHandler&& handler)
{
  StatusOr<i64> page_offset_in_file = this->get_file_offset_of_page(page_id);
  if (!page_offset_in_file.ok()) {
    handler(page_offset_in_file.status());
    return;
  }

  this->read_some(*page_offset_in_file, PageBuffer::allocate(this->page_size()),
                  /*n_read_so_far=*/0, std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::read_some(i64 page_offset_in_file,
                                     std::shared_ptr<PageBuffer>&& page_buffer, usize n_read_so_far,
                                     ReadHandler&& handler)
{
  BATT_CHECK_GE(page_offset_in_file, 0);

  BATT_CHECK_LE(n_read_so_far, page_buffer->size());
  MutableBuffer buffer = page_buffer->mutable_buffer() + n_read_so_far;

  this->file_.async_read_some(
      page_offset_in_file + n_read_so_far, buffer,
      bind_handler(
          std::move(handler), [this, page_offset_in_file, page_buffer = std::move(page_buffer),
                               n_read_so_far](ReadHandler&& handler, StatusOr<i32> result) mutable {
            if (!result.ok()) {
              if (batt::status_is_retryable(result.status())) {
                this->read_some(page_offset_in_file, std::move(page_buffer), n_read_so_far,
                                std::move(handler));
                return;
              }

              LOG(WARNING) << "IoRingPageFileDevice::read failed; page_offset_in_file+"
                           << n_read_so_far << "=" << page_offset_in_file + n_read_so_far
                           << " n_read_so_far=" << n_read_so_far
                           << " page_offset_in_file=" << page_offset_in_file;

              handler(result.status());
              return;
            }
            BATT_CHECK_GT(*result, 0) << "We must either make progress or receive an error code!";

            n_read_so_far += *result;

            // If we have reached the end of the buffer, invoke the handler.
            //
            if (n_read_so_far == page_buffer->size()) {
              handler(std::move(page_buffer));
              return;
            }

            // The write was short; write again from the new stop point.
            //
            this->read_some(page_offset_in_file, std::move(page_buffer), n_read_so_far,
                            std::move(handler));
          }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::drop(PageId id, WriteHandler&& handler)
{
  // TODO [tastolfi 2021-06-11] - trim at device level?
  (void)id;
  handler(OkStatus());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<u64> IoRingPageFileDevice::get_physical_page(PageId page_id) const
{
  const i64 physical_page = this->page_ids_.get_physical_page(page_id);
  if (physical_page > this->config_->page_count || physical_page < 0) {
    return Status{batt::StatusCode::kOutOfRange};  // TODO [tastolfi 2021-10-20] "the specified page
                                                   // is beyond the device max"
  }

  return physical_page;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingPageFileDevice::get_file_offset_of_page(PageId page_id) const
{
  const auto physical_page = this->get_physical_page(page_id);
  BATT_REQUIRE_OK(physical_page);

  return this->config_.absolute_page_0_offset() +
         (static_cast<i64>(*physical_page) << u16{this->config_->page_size_log2});
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
