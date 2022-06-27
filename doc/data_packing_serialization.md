# Data Packing/Serialization

## Summary

LLFS includes a no-copy data serialization (packing) and
deserialization system that aims to provide high-level data structures
to applications while minimizing overhead associated with copying,
allocation, and parsing.

This system makes heavy use
of
[Boost.Endian's int buffer types](https://www.boost.org/doc/libs/1_79_0/libs/endian/doc/html/endian.html#buffers).
For brevity within the LLFS code, these are aliased within
'llfs/int_types.hpp':

```c++
namespace llfs {

using big_u8 = boost::endian::big_uint8_t;
using big_u16 = boost::endian::big_uint16_t;
using big_u24 = boost::endian::big_uint24_t;
using big_u32 = boost::endian::big_uint32_t;
using big_u64 = boost::endian::big_uint64_t;

using big_i8 = boost::endian::big_int8_t;
using big_i16 = boost::endian::big_int16_t;
using big_i24 = boost::endian::big_int24_t;
using big_i32 = boost::endian::big_int32_t;
using big_i64 = boost::endian::big_int64_t;

using little_u8 = boost::endian::little_uint8_t;
using little_u16 = boost::endian::little_uint16_t;
using little_u24 = boost::endian::little_uint24_t;
using little_u32 = boost::endian::little_uint32_t;
using little_u64 = boost::endian::little_uint64_t;

using little_i8 = boost::endian::little_int8_t;
using little_i16 = boost::endian::little_int16_t;
using little_i24 = boost::endian::little_int24_t;
using little_i32 = boost::endian::little_int32_t;
using little_i64 = boost::endian::little_int64_t;

} // namespace llfs
```

LLFS refers to these types collectively as Packed Integer types.

LLFS currently does not define a IDL/DDL for describing structured
data schemas; rather it uses C++ struct types directly, with certain
restrictions.  A `struct` type that conforms to these type
restrictions is referred to as a Packed Struct type.

While some care is necessary to correctly serialize data into packed
form (using a composition of Packed Struct and Integer types), once it
is packed, there is no explicit deserialization or parsing step.
Application code may simply cast a byte buffer to the appropriate
packed type and read it directly.  Note that data structures obtained
in this way are strictly read-only (`const`).

## Packed Types

Packed Types in LLFS are defined recursively.  A type `T` is a Packed
Type from LLFS' perspective if any of the following is true:

- `T` is a Packed Integer (as defined above)
- `T` is `boost::uuids::uuid`
- `T` is a struct for which all fields:
  - Are some Packed Type `U`
  - Are aligned property relative to the struct start: a field type
    `U` of size N bytes must be aligned to the nearest N-byte boundary
    in general; if N > 8 (64-bits) then 64-bit alignment is considered
    sufficient; the 24-bit integer types defined above are required to
    be 32-bit aligned.
  
In the case of a Packed Struct, the restrictions listed above must
guarantee that the size of the struct as a whole is equal to the sum
of the sizes of the fields.  It is best practice to statically assert
that this is the case immediately after the declaration of a packed
type.  For example ('llfs/packed_bytes.hpp'):

```c++
namespace llfs {

struct PackedBytes {
  // Offset in bytes of the start of the packed data, relative to the start of
  // this record.  If this value is less than 8 (sizeof(PackedBytes)), then `data_size` is invalid;
  // in this case, the data is presumed to extend to the end of this record.
  //
  little_u24 data_offset;

  // Not used by this record.
  //
  little_u8 unused_[1];

  // The size in bytes of the packed data.  MAY BE INVALID; always use `PackedBytes::size()` when
  // reading size instead of accessing this field directly.
  //
  little_u24 data_size;

  // Reserved for future use.
  //
  little_u8 reserved_[1];
};

BATT_STATIC_ASSERT_EQ(sizeof(PackedBytes), 8);

} // namespace llfs
```

In addition, a Packed Type `T` must provide overloads of the following free functions:

* `usize packed_sizeof(const T &obj)` - must return the total byte
  size of the type, including any referenced data; in some cases it is
  possible for the size of a Packed Type instance to extend beyond the
  type itself (See "Fixed-Size vs Variable-Size Packed Types" below).
* `T* pack_object_to(const U& from, T* to, llfs::DataPacker* dst)` -
  must deep-copy `from` to `to`; `T` may be the same as `U`, in which
  case `T` is considered a Simple Packed Type (see below); if `T` and
  `U` are different types, then `T` is considered a Complex Packed
  Type (see below).
  
"Complex" Packed Types (see below) may additionally define:

* `llfs::StatusOr<U> unpack_object(const T &obj, llfs::DataReader* src)` -
  deserialize `obj` and return the in-memory (unpacked) representation of the data.
  
As mentioned earlier, it is not typically required to call
`unpack_object` to convert a Packed Type instance into its Unpacked
Type (`U`) in order to read it.  However, `unpack_object` may be used
to create a mutable object that can be used to create new/modified
structured data which can later be packed for storage.

### Fixed-Size vs Variable-Sized Packed Types

A Fixed-Size Packed Type `T` is defined as a Packed Type for which
`packed_sizeof` always returns the same as `sizeof(T)`.  An additional
generic overload of `packed_sizeof` is provided for such types
('llfs/data_layout.hpp'):

```c++
namespace llfs {

template <typename T>
inline constexpr usize packed_sizeof(batt::StaticType<T> = {})
{
  return sizeof(T);
}

} // namespace llfs
```

Examples of Variable-Sized Packed Types include:

- `llfs::PackedArray<T>`
- `llfs::PackedBytes`
- `llfs::PackedVariant<Ts...>`

### Simple vs Complex Packed Types

A Complex Packed Type is one that has a non-Packed type associated
with it, and defines methods for explicitly serializing/deserializing
the Unpacked Type representation to/from the Packed Type.  A Simple Packed
Type is one for which the packed (serialized) representation and the
in-memory/runtime (deserialized) representation is the same.

#### Defining a Simple Packed Type

Simple Packed Types can be defined by simply declaring a struct that
meets the requirements listed above and then using the
`LLFS_SIMPLE_PACKED_TYPE` macro (from 'llfs/simple_packed_type.hpp').  For example:

```c++
#include <llfs/int_types.hpp>
#include <llfs/simple_packed_type.hpp>

struct PackedSemanticVersion {
  llfs::big_u32 major;
  llfs::big_u16 minor;
  llfs::big_u16 patch;
};

LLFS_SIMPLE_PACKED_TYPE(PackedSemanticVersion);
```

This will define all the required free function overloads needed by
LLFS to pack/unpack this type.

#### Defining a Complex Packed Type

_TODO [tastolfi 2022-06-27] ..._
