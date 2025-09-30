//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_page_file_device.hpp>
//

#include <llfs/config.hpp>
#include <llfs/page_device_metrics.hpp>

#ifndef LLFS_DISABLE_IO_URING

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ auto IoRingPageFileDevice::PhysicalLayout::from_packed_config(
    const FileOffsetPtr<PackedPageDeviceConfig>& config) -> Self
{
  return Self{
      .page_size = PageSize{BATT_CHECKED_CAST(u32, config->page_size())},
      .page_count = PageCount{BATT_CHECKED_CAST(u32, config->page_count.value())},
      .page_0_offset = FileOffset{config.absolute_page_0_offset()},
      .page_size_log2 = BATT_CHECKED_CAST(u16, config->page_size_log2.value()),
      .is_last_in_file = config->is_last_in_file(),
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingPageFileDevice::IoRingPageFileDevice(
    IoRing::File&& file, const FileOffsetPtr<PackedPageDeviceConfig>& config) noexcept
    : is_read_only_{false}
    , is_sharded_view_{false}
    , shared_file_{std::make_shared<IoRing::File>(std::move(file))}
    , file_{*this->shared_file_}
    , physical_layout_{PhysicalLayout::from_packed_config(config)}
    , page_ids_{this->layout().page_count, config->device_id}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingPageFileDevice::IoRingPageFileDevice(
    page_device_id_int device_id, std::shared_ptr<IoRing::File>&& file,
    const PhysicalLayout& physical_layout) noexcept
    : is_read_only_{true}
    , is_sharded_view_{true}
    , shared_file_{std::move(file)}
    , file_{*this->shared_file_}
    , physical_layout_{physical_layout}
    , page_ids_{this->layout().page_count, device_id}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
std::unique_ptr<PageDevice> IoRingPageFileDevice::make_sharded_view(page_device_id_int device_id,
                                                                    PageSize shard_size)
{
  BATT_CHECK_NE(device_id, this->get_id());

  const u16 shard_size_log2 = BATT_CHECKED_CAST(u16, batt::log2_ceil(shard_size));
  BATT_CHECK_LT(shard_size_log2, this->layout().page_size_log2);

  const u16 shards_per_page_log2 = this->layout().page_size_log2 - shard_size_log2;

  auto sharded_layout = PhysicalLayout{
      .page_size = shard_size,
      .page_count = PageCount{this->layout().page_count << shards_per_page_log2},
      .page_0_offset = this->layout().page_0_offset,
      .page_size_log2 = shard_size_log2,
      .is_last_in_file = this->layout().is_last_in_file,
  };

  auto sharded_view_device = std::make_unique<IoRingPageFileDevice>(
      device_id, batt::make_copy(this->shared_file_), sharded_layout);

  return sharded_view_device;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageIdFactory IoRingPageFileDevice::page_ids()
{
  return this->page_ids_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
PageSize IoRingPageFileDevice::page_size()
{
  return this->layout().page_size;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<std::shared_ptr<PageBuffer>> IoRingPageFileDevice::prepare(PageId page_id)
{
  if (this->is_read_only_) {
    return {batt::StatusCode::kFailedPrecondition};
  }

  StatusOr<u64> physical_page = this->get_physical_page(page_id);
  BATT_REQUIRE_OK(physical_page);

  return PageBuffer::allocate(this->page_size(), page_id);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::write(std::shared_ptr<const PageBuffer>&& page_buffer,
                                 WriteHandler&& handler)
{
  if (this->is_read_only_) {
    handler(Status{batt::StatusCode::kFailedPrecondition});
    return;
  }

  StatusOr<i64> page_offset_in_file = this->get_file_offset_of_page(page_buffer->page_id());
  if (!page_offset_in_file.ok()) {
    handler(page_offset_in_file.status());
    return;
  }

  PageDeviceMetrics::instance()  //
      .write_count_per_page_size_log2[this->layout().page_size_log2]
      .add(1);

  BATT_CHECK_EQ(page_buffer->size(), get_page_size(page_buffer));
  BATT_CHECK_EQ(page_buffer->size(), this->page_size());

  const PackedPageHeader& page_header = get_page_header(*page_buffer);
  ConstBuffer remaining_data = [&] {
    ConstBuffer buffer = get_const_buffer(page_buffer);
    if (page_header.unused_end == page_header.size &&
        page_header.unused_begin < page_header.unused_end) {
      buffer = resize_buffer(
          buffer, batt::round_up_bits(kDirectIOBlockSizeLog2, page_header.unused_begin.value()));

      BATT_CHECK_GE(page_header.unused_begin.value(), sizeof(PackedPageHeader));
      BATT_CHECK_LE(buffer.size(), page_buffer->size());
    }
    return buffer;
  }();

  this->write_some(*page_offset_in_file, std::move(page_buffer), remaining_data,
                   std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::write_some(i64 page_offset_in_file,
                                      std::shared_ptr<const PageBuffer>&& page_buffer,
                                      ConstBuffer remaining_data, WriteHandler&& handler)
{
  BATT_CHECK(!this->is_read_only_);
  BATT_CHECK_GE(page_offset_in_file, 0);

  // If we have reached the end of the data, invoke the handler.
  //
  if (remaining_data.size() == 0u) {
    handler(OkStatus());
    return;
  }

  this->file_.async_write_some(
      page_offset_in_file, remaining_data,
      bind_handler(std::move(handler), [this, page_offset_in_file,
                                        page_buffer = std::move(page_buffer), remaining_data](
                                           WriteHandler&& handler, StatusOr<i32> result) mutable {
        if (!result.ok()) {
          if (batt::status_is_retryable(result.status())) {
            this->write_some(page_offset_in_file, std::move(page_buffer), remaining_data,
                             std::move(handler));
            return;
          }
          LLFS_LOG_WARNING() << "IoRingPageFileDevice::write failed;"
                             << BATT_INSPECT(page_offset_in_file);

          handler(result.status());
          return;
        }
        const i32 bytes_written = *result;

        BATT_CHECK_GT(bytes_written, 0) << "We must either make progress or receive an error code!";

        remaining_data += bytes_written;
        page_offset_in_file += bytes_written;

        // The write was short; write again from the new stop point.
        //
        this->write_some(page_offset_in_file, std::move(page_buffer), remaining_data,
                         std::move(handler));
      }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::read(PageId page_id, ReadHandler&& handler)
{
  LLFS_VLOG(1) << "IoRingPageFileDevice::read(page_id=" << page_id << ")";

  StatusOr<i64> page_offset_in_file = this->get_file_offset_of_page(page_id);
  if (!page_offset_in_file.ok()) {
    LLFS_VLOG(1) << "bad page offset: " << BATT_INSPECT(page_id)
                 << BATT_INSPECT(page_offset_in_file.status());
    handler(page_offset_in_file.status());
    return;
  }

  PageDeviceMetrics::instance()  //
      .read_count_per_page_size_log2[this->layout().page_size_log2]
      .add(1);

  const PageSize page_buffer_size = this->page_size();
  const usize n_read_so_far = 0;

  this->read_some(page_id, *page_offset_in_file, PageBuffer::allocate(page_buffer_size, page_id),
                  page_buffer_size, n_read_so_far, std::move(handler));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::read_some(PageId page_id, i64 page_offset_in_file,
                                     std::shared_ptr<PageBuffer>&& page_buffer,
                                     usize page_buffer_size, usize n_read_so_far,
                                     ReadHandler&& handler)
{
  BATT_CHECK_GE(page_offset_in_file, 0);
  BATT_CHECK_LE(n_read_so_far, page_buffer_size);

  MutableBuffer buffer = get_mutable_buffer(page_buffer) + n_read_so_far;

  this->file_.async_read_some(
      page_offset_in_file + n_read_so_far, buffer,
      bind_handler(
          std::move(handler),
          [this, page_id, page_offset_in_file, page_buffer = std::move(page_buffer),
           page_buffer_size, n_read_so_far](ReadHandler&& handler, StatusOr<i32> result) mutable {
            if (!result.ok()) {
              if (batt::status_is_retryable(result.status())) {
                this->read_some(page_id, page_offset_in_file, std::move(page_buffer),
                                page_buffer_size, n_read_so_far, std::move(handler));
                return;
              }

              LLFS_LOG_WARNING() << "IoRingPageFileDevice::read failed; page_offset_in_file+"
                                 << n_read_so_far << "=" << page_offset_in_file + n_read_so_far
                                 << " n_read_so_far=" << n_read_so_far
                                 << " page_offset_in_file=" << page_offset_in_file;

              handler(result.status());
              return;
            }
            BATT_CHECK_GT(*result, 0) << "We must either make progress or receive an error code!";

            // Sanity check the page header and fail fast if something looks wrong.
            //
            const usize n_read_before = n_read_so_far;
            n_read_so_far += *result;

            if (!this->is_sharded_view_ && (n_read_before < sizeof(PackedPageHeader) &&
                                            n_read_so_far >= sizeof(PackedPageHeader))) {
              Status status = get_page_header(*page_buffer)
                                  .sanity_check(PageSize{BATT_CHECKED_CAST(u32, page_buffer_size)},
                                                page_id, this->page_ids_);
              if (!status.ok()) {
                // If the only sanity check that failed was a bad generation number, then we report
                // page not found.
                //
                if (status == StatusCode::kPageHeaderBadGeneration) {
                  status = batt::StatusCode::kNotFound;
                }
                handler(status);
                return;
              }
              VLOG(1) << "Short read: " << BATT_INSPECT(page_id)
                      << BATT_INSPECT(page_offset_in_file) << BATT_INSPECT(page_buffer_size)
                      << BATT_INSPECT(n_read_so_far);
            }

            // If we have reached the end of the buffer, invoke the handler.  Success!
            //
            if (n_read_so_far == page_buffer_size) {
              handler(std::move(page_buffer));
              return;
            }

            // The write was short; write again from the new stop point.
            //
            this->read_some(page_id, page_offset_in_file, std::move(page_buffer), page_buffer_size,
                            n_read_so_far, std::move(handler));
          }));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingPageFileDevice::drop(PageId id, WriteHandler&& handler)
{
  if (this->is_read_only_) {
    handler(Status{batt::StatusCode::kFailedPrecondition});
    return;
  }

  // TODO [tastolfi 2021-06-11] - trim at device level?
  // Note that we are not clearing the data blocks here. This behavior is copied replicated for
  // SimulatedPageDevice drop() too. Thus, when modifying this function do visit
  // SimulatedPageDevice::Impl::drop() and make sure things are updated there.
  (void)id;
  handler(OkStatus());
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingPageFileDevice::is_last_in_file() const
{
  return this->layout().is_last_in_file;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<u64> IoRingPageFileDevice::get_physical_page(PageId page_id) const
{
  const i64 physical_page = this->page_ids_.get_physical_page(page_id);
  if (physical_page < 0 || !(physical_page < static_cast<i64>(this->layout().page_count))) {
    return Status{batt::StatusCode::kOutOfRange};
  }

  return physical_page;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<i64> IoRingPageFileDevice::get_file_offset_of_page(PageId page_id) const
{
  const StatusOr<u64> physical_page = this->get_physical_page(page_id);
  BATT_REQUIRE_OK(physical_page);

  return this->layout().page_0_offset +
         (static_cast<i64>(*physical_page) << this->layout().page_size_log2);
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
