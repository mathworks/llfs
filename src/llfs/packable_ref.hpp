//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PACKABLE_REF_HPP
#define LLFS_PACKABLE_REF_HPP

#include <llfs/data_packer.hpp>
#include <llfs/data_reader.hpp>
#include <llfs/int_types.hpp>
#include <llfs/pack_as_raw.hpp>
#include <llfs/page_id.hpp>
#include <llfs/ref.hpp>
#include <llfs/seq.hpp>
#include <llfs/slot.hpp>
#include <llfs/slot_reader.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class PackableRef
{
 public:
  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  class AbstractPackable
  {
   public:
    AbstractPackable() = default;
    AbstractPackable(const AbstractPackable&) = delete;
    AbstractPackable& operator=(const AbstractPackable&) = delete;

    virtual ~AbstractPackable() = default;

    virtual usize packed_size(const void*) const
    {
      return 0;
    }

    virtual PackedRawData* pack_into(const void*, DataPacker* /*packer*/) const
    {
      return nullptr;
    }

    virtual BoxedSeq<PageId> trace_refs_impl(const void*) const
    {
      return seq::Empty<PageId>{} | seq::boxed();
    }

    virtual const char* name_of_type() const
    {
      return "?";
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <typename T>
  class PackableImpl : public AbstractPackable
  {
   public:
    usize packed_size(const void* ptr) const override
    {
      return packed_sizeof(*reinterpret_cast<const T*>(ptr));
    }

    PackedRawData* pack_into(const void* ptr, DataPacker* packer) const override
    {
      return reinterpret_cast<PackedRawData*>(
          pack_object(*reinterpret_cast<const T*>(ptr), packer));
    }

    const char* name_of_type() const override
    {
      return batt::name_of<T>();
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -
  //
  template <typename T>
  class PackableImplWithTraceRefs : public PackableImpl<T>
  {
   public:
    using PackableImpl<T>::PackableImpl;

    BoxedSeq<PageId> trace_refs_impl(const void* ptr) const override
    {
      return trace_refs(*reinterpret_cast<const T*>(ptr));
    }

    const char* name_of_type() const override
    {
      return batt::name_of<T>();
    }
  };

  //==#==========+==+=+=++=+++++++++++-+-+--+----- --- -- -  -  -   -

  static const AbstractPackable* null_impl()
  {
    const static AbstractPackable impl_;
    return &impl_;
  }

  template <typename T>
  static const AbstractPackable* impl(...)
  {
    const static PackableImpl<T> impl_;
    return &impl_;
  }

  template <typename T, typename = std::enable_if_t<std::is_same_v<
                            BoxedSeq<PageId>, decltype(trace_refs(std::declval<const T&>()))>>>
  static const AbstractPackable* impl(int)
  {
    const static PackableImplWithTraceRefs<T> impl_;
    return &impl_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PackableRef() noexcept : impl_{PackableRef::null_impl()}, ptr_{nullptr}
  {
  }

  PackableRef(const PackableRef&) = default;
  PackableRef& operator=(const PackableRef&) = default;
  ~PackableRef() = default;

  template <typename T,                                                    //
            typename = batt::EnableIfNoShadow<PackableRef, T>,             //
            typename = decltype(packed_sizeof(std::declval<const T&>())),  //
            typename = decltype(pack_object(std::declval<const T&>(),
                                            std::declval<DataPacker*>()))  //
            >
  explicit PackableRef(const T& obj_ref) noexcept : impl_{PackableRef::impl<T>(0)}
                                                  , ptr_{&obj_ref}
  {
  }

  usize packed_size() const
  {
    return this->impl_->packed_size(this->ptr_);
  }

  PackedRawData* pack_into(DataPacker* packer) const
  {
    return this->impl_->pack_into(this->ptr_, packer);
  }

  BoxedSeq<PageId> trace_refs_impl() const
  {
    return this->impl_->trace_refs_impl(this->ptr_);
  }

  const char* name_of_type() const
  {
    return this->impl_->name_of_type();
  }

 private:
  const AbstractPackable* impl_;
  const void* ptr_;
};

usize packed_sizeof(const PackableRef& p);

inline PackedRawData* pack_object(const PackableRef& p, DataPacker* dst)
{
  return p.pack_into(dst);
}

inline BoxedSeq<PageId> trace_refs(const PackableRef& p)
{
  return p.trace_refs_impl();
}

LLFS_DEFINE_PACKED_TYPE_FOR(PackableRef, PackedRawData);

}  // namespace llfs

#endif  // LLFS_PACKABLE_REF_HPP
