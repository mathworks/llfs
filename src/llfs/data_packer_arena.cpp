//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#include <llfs/data_packer_arena.hpp>
//
#include <llfs/data_packer.hpp>
#include <llfs/varint.hpp>

#include <batteries/checked_cast.hpp>

namespace llfs {

namespace {

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
MutableBuffer buffer_from(const boost::iterator_range<u8*>& byte_range)
{
  return MutableBuffer{byte_range.begin(), byte_range.size()};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<u8*> byte_range_from(const MutableBuffer& buffer)
{
  u8* const data = static_cast<u8*>(buffer.data());
  return boost::iterator_range<u8*>{data, data + buffer.size()};
}

}  //namespace

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
/*explicit*/ DataPackerArena::DataPackerArena(DataPacker* packer,
                                              boost::iterator_range<u8*> avail) noexcept
    : capacity_{avail.size()}
    , packer_{packer}
    , avail_{avail}
    , full_{false}
{
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
DataPacker* DataPackerArena::get_packer() const
{
  return this->packer_.get();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize DataPackerArena::space() const
{
  return this->avail_.size();
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
usize DataPackerArena::capacity() const
{
  return this->capacity_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
bool DataPackerArena::full() const
{
  return this->full_;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Interval<isize> DataPackerArena::unused() const
{
  BATT_CHECK_NOT_NULLPTR(this->packer_.get());

  u8* const buffer_begin = this->packer_->buffer_begin();

  return Interval<isize>{
      this->avail_.begin() - buffer_begin,
      this->avail_.end() - buffer_begin,
  };
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void DataPackerArena::invalidate()
{
  this->capacity_ = 0;
  this->packer_.reset();
  this->avail_ = boost::iterator_range<u8*>{nullptr, nullptr};
  this->full_ = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
void DataPackerArena::set_full()
{
  this->full_ = true;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<DataPackerArena> DataPackerArena::reserve_front(usize size)
{
  Optional<MutableBuffer> buf = this->allocate_front(size);
  if (!buf) {
    return None;
  }
  return DataPackerArena{this->get_packer(), byte_range_from(*buf)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<DataPackerArena> DataPackerArena::reserve_back(usize size)
{
  Optional<MutableBuffer> buf = this->allocate_back(size);
  if (!buf) {
    return None;
  }
  return DataPackerArena{this->get_packer(), byte_range_from(*buf)};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<MutableBuffer> DataPackerArena::allocate_front(usize size)
{
  if (size > this->space()) {
    this->full_ = true;
    return None;
  }

  return buffer_from(this->nocheck_alloc_front(BATT_CHECKED_CAST(isize, size)));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
Optional<MutableBuffer> DataPackerArena::allocate_back(usize size)
{
  if (size > this->space()) {
    this->full_ = true;
    return None;
  }

  return buffer_from(this->nocheck_alloc_back(BATT_CHECKED_CAST(isize, size)));
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
u8* DataPackerArena::pack_varint(u64 n)
{
  if (this->full_) {
    return nullptr;
  }

  // Attempt to pack the varint to the front of the available range.
  //
  u8* const dst_begin = this->avail_.begin();
  u8* const avail_end = this->avail_.end();
  u8* dst_end = pack_varint_to(dst_begin, avail_end, n);
  if (dst_end == nullptr) {
    this->full_ = true;
    return nullptr;
  }

  // Commit the packed bytes.
  //
  this->avail_ = boost::iterator_range<u8*>{dst_end, avail_end};

  return dst_end;
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<u8*> DataPackerArena::nocheck_alloc_front(isize size)
{
  u8* const buf_begin = this->avail_.begin();
  this->avail_.advance_begin(size);
  u8* const buf_end = this->avail_.begin();

  return boost::iterator_range<u8*>{buf_begin, buf_end};
}

//==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
//
boost::iterator_range<u8*> DataPackerArena::nocheck_alloc_back(isize size)
{
  u8* const buf_end = this->avail_.end();
  this->avail_.advance_end(-size);
  u8* const buf_begin = this->avail_.end();

  return boost::iterator_range<u8*>{buf_begin, buf_end};
}

}  //namespace llfs
