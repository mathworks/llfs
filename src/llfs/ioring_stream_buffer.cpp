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

#include <batteries/hint.hpp>

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
usize IoRingStreamBuffer::size() const noexcept
{
  // NOTE: we must read these in this order to make sure we don't (falsely) observe an
  // inconsistent state due to race conditions.
  //
  const i64 observed_consume_pos = this->consume_pos_.get_value();
  const i64 observed_commit_pos = this->commit_pos_.get_value();

  BATT_CHECK_GE(observed_commit_pos, observed_consume_pos);

  // Clamp to the known maximum capacity.
  //
  return std::min(this->max_size(),
                  BATT_CHECKED_CAST(usize, observed_commit_pos - observed_consume_pos));
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
StatusOr<IoRingMutableBufferView> IoRingStreamBuffer::prepare()
{
  LLFS_VLOG(1) << "minimum consume pos reached; allocating buffer...";

  if (this->end_of_stream_.load()) {
    return {batt::StatusCode::kClosed};
  }

  // Allocate a buffer from the pool.
  //
  BATT_REQUIRE_OK(this->initialize());
  BATT_ASSIGN_OK_RESULT(IoRingBufferPool::Buffer buffer,
                        this->private_buffer_pool_->await_allocate());

  LLFS_VLOG(1) << "buffer allocated;" << BATT_INSPECT(buffer.size());

  MutableBuffer slice = buffer.get();

  return IoRingMutableBufferView{std::move(buffer), slice};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingStreamBuffer::commit(BufferView&& view)
{
  BATT_CHECK_EQ(std::addressof(view.buffer.pool()), std::addressof(*this->private_buffer_pool_))
      << "IoRingStreamBuffer::commit only accepts buffer view objects for buffers returned from "
         "IoRingStreamBuffer::prepare on the same stream.";

  usize byte_count = view.slice.size();
  {
    batt::ScopedLock<Fragment> locked{this->queue_};

    locked->push(std::move(view));

    BATT_CHECK_LE(locked->view_count(), this->queue_capacity_);
  }
  this->commit_pos_.fetch_add(byte_count);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingStreamBuffer::consume(i64 start, i64 end) -> StatusOr<Fragment>
{
  LLFS_VLOG(1) << "IoRingStreamBuffer::consume(" << start << ", " << end << ")"
               << BATT_INSPECT(this->commit_pos_.get_value());

  // Wait for the target range to be committed (i.e., wait for the commit pos to be >= end).
  //
  StatusOr<i64> final_commit_pos = this->commit_pos_.await_true([&](i64 observed_commit_pos) {
    return observed_commit_pos - end >= 0;
  });
  BATT_REQUIRE_OK(final_commit_pos);

  LLFS_VLOG(1)
      << "IoRingStreamBuffer::consume() commit_pos reached; waiting for all prev consumers to "
         "finish...";

  // Wait for all consumers of lower ranges to update the consume pos.
  //
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
    this->check_for_end_of_stream(locked);
  }
  this->consume_pos_.set_value(end);

  return result;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
auto IoRingStreamBuffer::consume_some() -> StatusOr<Fragment>
{
  const i64 observed_consume_pos = this->consume_pos_.get_value();

  // Wait for the commit pos to advance past the consume pos.
  //
  StatusOr<i64> final_commit_pos =
      this->commit_pos_.await_true([&observed_consume_pos](i64 observed_commit_pos) {
        return observed_commit_pos - observed_consume_pos > 0;
      });

  if (BATT_HINT_FALSE(final_commit_pos.status() == batt::StatusCode::kClosed)) {
    const i64 observed_commit_pos = this->commit_pos_.get_value();
    if (observed_commit_pos - observed_consume_pos > 0) {
      BATT_UNTESTED_LINE();
      final_commit_pos = observed_commit_pos;
    }
  }
  BATT_REQUIRE_OK(final_commit_pos);

  Fragment result;
  {
    batt::ScopedLock<Fragment> locked{this->queue_};

    std::swap(result, *locked);
    this->consume_pos_.fetch_add(BATT_CHECKED_CAST(i64, result.byte_size()));
    // ^^
    //  We must check for end-of-stream *after* updating consume_pos to make sure no data is
    //  dropped at the end of the stream.
    //   vv
    this->check_for_end_of_stream(locked);

    if (BATT_HINT_FALSE(result.empty() && this->commit_pos_.is_closed())) {
      BATT_UNTESTED_LINE();
      return {batt::StatusCode::kClosed};
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
void IoRingStreamBuffer::check_for_end_of_stream(batt::ScopedLock<Fragment>& locked)
{
  if (this->end_of_stream_.load() && locked->empty()) {
    this->commit_pos_.close();
    this->consume_pos_.close();
  }
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
void IoRingStreamBuffer::Fragment::push(const Fragment& fragment)
{
  for (const BufferView& part : fragment.views_) {
    this->push(batt::make_copy(part));
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void IoRingStreamBuffer::Fragment::push(Fragment&& fragment)
{
  for (BufferView& part : fragment.views_) {
    this->push(std::move(part));
  }
  fragment.views_.clear();
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
        this_view.buffer,
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
  return this->as_seq()  //
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
