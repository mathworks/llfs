//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_FILE_OFFSET_PTR_HPP
#define LLFS_FILE_OFFSET_PTR_HPP

#include <llfs/config.hpp>
#include <llfs/filesystem.hpp>
#include <llfs/int_types.hpp>
#include <llfs/packed_config.hpp>
#include <llfs/raw_block_file.hpp>

#ifndef LLFS_DISABLE_IO_URING
#include <llfs/ioring_file.hpp>
#endif  // LLFS_DISABLE_IO_URING

#include <batteries/checked_cast.hpp>

#include <boost/preprocessor/cat.hpp>

#include <cstddef>
#include <type_traits>

namespace llfs {

template <typename T>
struct FileOffsetPtr {
  FileOffsetPtr() = default;

  template <typename U,
            typename = std::enable_if_t<std::is_lvalue_reference_v<U> &&
                                        std::is_same_v<std::decay_t<T>, std::decay_t<U>>>>
  /*implicit*/ FileOffsetPtr(const FileOffsetPtr<U>& other) noexcept
      : object{other.object}
      , file_offset{other.file_offset}
  {
  }

  FileOffsetPtr(T object, i64 file_offset) noexcept : object{object}, file_offset{file_offset}
  {
  }

  // The object stored at the given file_offset.
  //
  T object;

  // The **absolute** offset from the beginning of the file at which `object` resides.
  //
  i64 file_offset;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  i64 absolute_from_relative_offset(i64 relative_offset) const
  {
    return this->file_offset + relative_offset;
  }

  i64 relative_from_absolute_offset(i64 absolute_offset) const
  {
    return absolute_offset - this->file_offset;
  }

  FileOffsetPtr<std::decay_t<T>> decay_copy() const
  {
    return {this->object, this->file_offset};
  }

  FileOffsetPtr<const PackedConfigSlot&> get_slot(usize index) const
  {
    BATT_STATIC_ASSERT_TYPE_EQ(std::decay_t<T>, PackedConfigBlock);
    BATT_CHECK_LT(index, this->object.slots.size());

    const PackedConfigSlot& slot_ref = this->object.slots[index];

    return FileOffsetPtr<const PackedConfigSlot&>{
        slot_ref,
        this->file_offset + byte_distance(&this->object, &slot_ref),
    };
  }

  FileOffsetPtr<PackedConfigSlot&> mutable_slot(usize index) const
  {
    BATT_STATIC_ASSERT_TYPE_EQ(std::decay_t<T>, PackedConfigBlock);
    BATT_CHECK_LT(index, this->object.slots.size());

    PackedConfigSlot& slot_ref = this->object.slots[index];

    return FileOffsetPtr<PackedConfigSlot&>{
        slot_ref,
        this->file_offset + byte_distance(&this->object, &slot_ref),
    };
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  std::remove_reference_t<T>& operator*()
  {
    return this->object;
  }

  const std::decay_t<T>& operator*() const
  {
    return this->object;
  }

  std::remove_reference_t<T>* get()
  {
    return &this->object;
  }

  const std::decay_t<T>* get() const
  {
    return &this->object;
  }

  std::remove_reference_t<T>* operator->()
  {
    return this->get();
  }

  const std::decay_t<T>* operator->() const
  {
    return this->get();
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  Status read_from_fd(int fd)
  {
    StatusOr<ConstBuffer> status = read_fd(fd, batt::mutable_buffer_from_struct(this->object),
                                           batt::checked_cast<u64>(this->file_offset));
    BATT_REQUIRE_OK(status);
    return OkStatus();
  }

  Status write_to_fd(int fd)
  {
    return write_fd(fd, batt::buffer_from_struct(this->object),
                    batt::checked_cast<u64>(this->file_offset));
  }

#ifndef LLFS_DISABLE_IO_URING
  Status read_from_ioring_file(IoRing::File& ioring_file)
  {
    return ioring_file.read_all(this->file_offset, batt::mutable_buffer_from_struct(this->object));
  }

  Status write_to_ioring_file(IoRing::File& ioring_file)
  {
    return ioring_file.write_all(this->file_offset, batt::buffer_from_struct(this->object));
  }
#endif  // LLFS_DISABLE_IO_URING

  //+++++++++++-+-+--+----- --- -- -  -  -   -

#define LLFS_DELEGATE_FIELD_REBASE(NAME)                                                           \
  i64 BOOST_PP_CAT(absolute_, NAME)() const                                                        \
  {                                                                                                \
    static_assert(std::is_same_v<std::decay_t<decltype(this->object.NAME)>, little_i64> ||         \
                      std::is_same_v<std::decay_t<decltype(this->object.NAME)>, big_i64>,          \
                  "");                                                                             \
    return this->absolute_from_relative_offset(this->object.NAME);                                 \
  }                                                                                                \
                                                                                                   \
  void BOOST_PP_CAT(absolute_, NAME)(i64 absolute_offset)                                          \
  {                                                                                                \
    static_assert(std::is_same_v<std::decay_t<decltype(this->object.NAME)>, little_i64> ||         \
                      std::is_same_v<std::decay_t<decltype(this->object.NAME)>, big_i64>,          \
                  "");                                                                             \
    this->object.NAME = this->relative_from_absolute_offset(absolute_offset);                      \
  }

  LLFS_DELEGATE_FIELD_REBASE(page_0_offset)
  LLFS_DELEGATE_FIELD_REBASE(block_0_offset)
  LLFS_DELEGATE_FIELD_REBASE(offset)
  LLFS_DELEGATE_FIELD_REBASE(begin_offset)
  LLFS_DELEGATE_FIELD_REBASE(end_offset)
  LLFS_DELEGATE_FIELD_REBASE(prev_offset)
  LLFS_DELEGATE_FIELD_REBASE(next_offset)

#undef LLFS_DELEGATE_FIELD_REBASE

#define LLFS_SUBFIELD_PTR(NAME)                                                                    \
  decltype(auto) BOOST_PP_CAT(get_, NAME)() const                                                  \
  {                                                                                                \
    using FieldType = decltype(this->object.NAME);                                                 \
    static_assert(sizeof(std::decay_t<FieldType>) < 0x10000000000ull, "");                         \
    using FieldRef = const std::decay_t<FieldType>&;                                               \
    return FileOffsetPtr<FieldRef>{                                                                \
        .object = this->object.NAME,                                                               \
        .file_offset = this->file_offset + static_cast<i64>(offsetof(std::decay_t<T>, NAME)),      \
    };                                                                                             \
  }                                                                                                \
                                                                                                   \
  decltype(auto) BOOST_PP_CAT(mutable_, NAME)()                                                    \
  {                                                                                                \
    using FieldType = decltype(this->object.NAME);                                                 \
    static_assert(sizeof(std::decay_t<FieldType>) < 0x10000000000ull, "");                         \
    using FieldRef = std::decay_t<FieldType>&;                                                     \
    return FileOffsetPtr<FieldRef>{                                                                \
        .object = this->object.NAME,                                                               \
        .file_offset = this->file_offset + static_cast<i64>(offsetof(std::decay_t<T>, NAME)),      \
    };                                                                                             \
  }

  LLFS_SUBFIELD_PTR(page_allocator)
  LLFS_SUBFIELD_PTR(log_device)
  LLFS_SUBFIELD_PTR(page_device)
  LLFS_SUBFIELD_PTR(root_log)
  LLFS_SUBFIELD_PTR(recycler_log)

#undef LLFS_SUBFIELD_PTR
};

template <typename T>
inline std::ostream& operator<<(std::ostream& out, const FileOffsetPtr<T>& t)
{
  return out << "FileOffsetPtr{.file_offset=" << t.file_offset << ", .object=" << t.object << ",}";
}

}  // namespace llfs

#endif  // LLFS_FILE_OFFSET_PTR_HPP
