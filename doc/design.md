# LLFS Design Doc

_WARNING (2022/10/27) - This document expresses ideas that may be obsolete and may not reflect the current design of LLFS._

## Goals

- Run equally well on top of existing filesystems and raw block storage devices
- Acceptable performance on legacy (SATA, HDD) storage hardware, while targetting highly parallel solid-state devices (SSD, NAND Flash, 3D crosspoint)
- Minimize external dependencies; be as self-contained and complete as possible/practical
- Codify best practice; provide a platform for invention (bleeding-edge ideas belong in a layer above LLFS)

## Bag-of-Stuff Design

### Big Idea

Hand LLFS a list of files and raw block devices (we will just call these "files" from here on) and it scans them and figures out what it has.  Tells you if it is incomplete, allows you to continue, or repair (for example, a file or device is missing or has moved).

### Implementation Strategy

#### Diagram Conventions
- _All fields are unsigned integers by default_
- _Right-justified means big-endian_
- _Left-justified means little-endian_
- _Center-justified means raw bytes (usually ASCII text)_
- _`#`-fill means reserved for future use_

#### Types of Stored Data

There are three kinds of stored data in LLFS:

1. Configs (4kib)
2. Page Devices
3. Log Devices

A Config stores metadata that identifies what kind of information is stored in files.

A Page Device is an array of same-sized "pages."  Page sizes are always powers of 2.  The minimum page size is 512 bytes.  

A Log Device is an append-only sequence of data.  In durable storage, Logs have a head, or `flush_pos`, and a tail `trim_pos`.  The amount of active data, or _size_, of a log at any time is defined by `flush_pos` - `trim_pos`.  This is bounded by a fixed limit per Log; this limit is the _capacity_ of the Log.  So while the head of the Log can increase indefinitely, the log must be "trimmed" in order to maintain this invariant.  Trimming simply means to advance the `trim_pos`, discarding old data.  Within a file, a Log's active data is represented as a series of "flush blocks," which act like a ring buffer.  Each flush block is the same size, which is some multiple of 4kib.  

#### Configs

Each file starts with a 4k configuration header.  All configuration headers start with the same 64 byte format:

```
      +0             +8              +16             +24              
     ┌───────────────┬───────────────┬───────────────────────────────┐
    0│          magic│        version│        header type tag        │
     ├───────────────┴───────────────┴───────────────┬───────────────┤
   32│###############################################│crc64          │
     └───────────────────────────────────────────────┴───────────────┘
```

- **magic**: must be `0x0c96f901ff0a80e0` to identify this as a generic config header.
- **version**: the version of the common header; a big-endian unsigned 64-bit integer (`big_u64`); the most signficant 32-bits are the major version; the least significant 32-bits are split into most significant 16 bits (minor version) and least significant 16-bits (patch/fix number).  
- **header type tag**: an ASCII string that identifies the format of the remaining 4096 - 64 bytes of the header block.
- **crc64**: a CRC-64 checksum of the *entire* 4096-byte block, with the crc64 field initially set to zero.


