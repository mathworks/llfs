//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/ioring.hpp>
//

#include <llfs/config.hpp>

#ifndef LLFS_DISABLE_IO_URING

#include <llfs/logging.hpp>

#include <batteries/assert.hpp>

#include <sys/stat.h>
#include <sys/types.h>

#include <errno.h>
#include <fcntl.h>
#include <cstdlib>

namespace llfs {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<IoRing> IoRing::make_new(MaxQueueDepth entries) noexcept
{
  BATT_ASSIGN_OK_RESULT(std::unique_ptr<Impl> impl, Impl::make_new(entries));

  return IoRing{std::move(impl)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
IoRing::IoRing(std::unique_ptr<Impl>&& impl) noexcept : impl_{std::move(impl)}
{
  BATT_CHECK(this->impl_->is_valid());
}

//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*static*/ StatusOr<ScopedIoRing> ScopedIoRing::make_new(MaxQueueDepth entries,
                                                         ThreadPoolSize n_threads) noexcept
{
  StatusOr<IoRing> io = IoRing::make_new(entries);
  BATT_REQUIRE_OK(io);

  return ScopedIoRing{std::make_unique<Impl>(std::move(*io), n_threads)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ScopedIoRing::ScopedIoRing(std::unique_ptr<Impl>&& impl) noexcept
    : impl_{std::move(impl)}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const IoRing& ScopedIoRing::get_io_ring() const
{
  BATT_ASSERT_NOT_NULLPTR(this->impl_);
  return this->impl_->get_io_ring();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::halt()
{
  if (this->impl_) {
    this->impl_->halt();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::join()
{
  if (this->impl_) {
    this->impl_->join();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ ScopedIoRing::Impl::Impl(IoRing&& io, ThreadPoolSize n_threads) noexcept
    : io_{std::move(io)}
    , threads_{}
    , halted_{false}
{
  // IMPORTANT: we must call on_work_started before launching threads so that `IoRing::run`
  // doesn't exit prematurely.
  //
  this->io_.on_work_started();

  for (usize i = 0; i < n_threads; ++i) {
    this->threads_.emplace_back([this] {
      this->io_thread_main();
    });
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
ScopedIoRing::Impl::~Impl() noexcept
{
  this->halt();
  this->join();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::Impl::io_thread_main()
{
  Status status = this->io_.run();
  LLFS_VLOG(1) << "ScopedIoRing::io_thread_main() exited with " << BATT_INSPECT(status);
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
const IoRing& ScopedIoRing::Impl::get_io_ring()
{
  return this->io_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::Impl::halt()
{
  const bool halted_previously = this->halted_.exchange(true);
  if (!halted_previously) {
    this->io_.on_work_finished();
    this->io_.stop();
  }
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void ScopedIoRing::Impl::join()
{
  for (std::thread& t : this->threads_) {
    t.join();
  }
}

}  // namespace llfs

#endif  // LLFS_DISABLE_IO_URING
