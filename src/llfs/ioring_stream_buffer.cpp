//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring_stream_buffer.hpp>
//
#include <llfs/logging.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class IoRingStreamBuffer

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ IoRingStreamBuffer::IoRingStreamBuffer(IoRingBufferPool& buffer_pool)
    : shared_buffer_pool_{buffer_pool}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Status IoRingStreamBuffer::initialize() noexcept
{
  if (!this->private_buffer_pool_) {
    BATT_ASSIGN_OK_RESULT(
        IoRingBufferPool::BufferVec buffers,
        this->shared_buffer_pool_.await_allocate(BufferCount{this->queue_capacity_}));

    this->private_buffer_pool_.emplace(std::move(buffers));
  }
  return batt::OkStatus();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingStreamBuffer::close()
{
  {
    batt::ScopedLock<Fragment> locked{this->queue_};
    this->end_of_stream_.store(true);
    this->check_for_end_of_stream(locked);
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
StatusOr<IoRingBufferPool::Buffer> IoRingStreamBuffer::prepare()
{
  BATT_REQUIRE_OK(this->queue_space_.await_not_equal(0));

  LLFS_VLOG(1) << "minimum consume pos reached; allocating buffer...";

  // Allocate a buffer from the pool.
  //
  BATT_REQUIRE_OK(this->initialize());
  BATT_ASSIGN_OK_RESULT(IoRingBufferPool::Buffer buffer,
                        this->private_buffer_pool_->await_allocate());

  LLFS_VLOG(1) << "buffer allocated;" << BATT_INSPECT(buffer.size());

  return buffer;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingStreamBuffer::commit(BufferView&& view)
{
  usize byte_count = view.slice.size();
  {
    batt::ScopedLock<Fragment> locked{this->queue_};

    locked->push(std::move(view));

    BATT_CHECK_LE(locked->view_count(), this->queue_capacity_);
    this->queue_space_.set_value(this->queue_capacity_ - locked->view_count());
  }
  this->commit_pos_.fetch_add(byte_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingStreamBuffer::consume(i64 start, i64 end) -> StatusOr<Fragment>
{
  LLFS_VLOG(1) << "IoRingStreamBuffer::consume(" << start << ", " << end << ")"
               << BATT_INSPECT(this->commit_pos_.get_value());

  StatusOr<i64> final_commit_pos = this->commit_pos_.await_true([&](i64 observed_commit_pos) {
    return observed_commit_pos >= end;
  });

  BATT_REQUIRE_OK(final_commit_pos);

  LLFS_VLOG(1)
      << "IoRingStreamBuffer::consume() commit_pos reached; waiting for all prev consumers to "
         "finish...";

  Status consume_pos_reached = this->consume_pos_.await_equal(start);
  BATT_REQUIRE_OK(consume_pos_reached);

  LLFS_VLOG(1) << "IoRingStreamBuffer::consume() grabbing data and returning!"
               << BATT_INSPECT(this->consume_pos_.get_value());

  Fragment result;
  {
    batt::ScopedLock<Fragment> locked{this->queue_};

    BATT_CHECK_GE(end, start);
    const usize n_to_pop = end - start;

    result = locked->pop(n_to_pop);

    if (!this->check_for_end_of_stream(locked)) {
      BATT_CHECK_LE(locked->view_count(), this->queue_capacity_);
      this->queue_space_.set_value(this->queue_capacity_ - locked->view_count());
    }
  }
  this->consume_pos_.set_value(end);

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingStreamBuffer::consume_some() -> StatusOr<Fragment>
{
  BATT_REQUIRE_OK(this->queue_space_.await_not_equal(this->queue_capacity_));

  Fragment result;
  {
    batt::ScopedLock<Fragment> locked{this->queue_};

    std::swap(result, *locked);
    this->consume_pos_.fetch_add(BATT_CHECKED_CAST(i64, result.byte_size()));
    if (!this->check_for_end_of_stream(locked)) {
      this->queue_space_.set_value(this->queue_capacity_);
    }
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize IoRingStreamBuffer::buffer_size() const noexcept
{
  return this->shared_buffer_pool_.buffer_size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingStreamBuffer::check_for_end_of_stream(batt::ScopedLock<Fragment>& locked)
{
  if (this->end_of_stream_.load() && locked->empty()) {
    this->queue_space_.set_value(0);
    this->queue_space_.close();
    this->commit_pos_.close();
    this->consume_pos_.close();
    return true;
  }
  return false;
}

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
// class IoRingStreamBuffer::Fragment

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool IoRingStreamBuffer::Fragment::empty() const noexcept
{
  return this->views_.empty();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize IoRingStreamBuffer::Fragment::view_count() const noexcept
{
  return this->views_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingStreamBuffer::Fragment::push(BufferView&& view)
{
  if (!this->views_.empty() && this->views_.back().merge_with(view)) {
    return;
  }
  this->views_.emplace_back(std::move(view));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingStreamBuffer::Fragment::pop(usize max_byte_count) -> Fragment
{
  usize bytes_popped = 0;
  Fragment result;

  while (!this->views_.empty() && bytes_popped < max_byte_count) {
    BufferView& this_view = this->views_.front();

    const usize bytes_this_view = std::min(this_view.slice.size(), max_byte_count - bytes_popped);

    result.views_.emplace_back(BufferView{
        .buffer = this_view.buffer,
        .slice =
            ConstBuffer{
                this_view.slice.data(),
                bytes_this_view,
            },
    });

    bytes_popped += bytes_this_view;
    this_view.slice += bytes_this_view;

    if (this_view.slice.size() == 0) {
      this->views_.erase(this->views_.begin());
    }
  }

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize IoRingStreamBuffer::Fragment::byte_size() const noexcept
{
  return batt::as_seq(this->views_)  //
         | batt::seq::map([](const BufferView& view) -> usize {
             return view.slice.size();
           })  //
         | batt::seq::sum();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ConstBuffer IoRingStreamBuffer::Fragment::gather_impl(MutableBuffer dst) const noexcept
{
  const void* dst_begin = dst.data();
  usize n_copied = 0;

  for (const BufferView& view : this->views_) {
    const usize n_to_copy = std::min(view.slice.size(), dst.size());
    std::memcpy(dst.data(), view.slice.data(), n_to_copy);
    dst += n_to_copy;
    n_copied += n_to_copy;
  }

  return ConstBuffer{dst_begin, n_copied};
}

}  //namespace llfs
