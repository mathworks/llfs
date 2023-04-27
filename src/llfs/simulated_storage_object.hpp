//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_SIMULATED_STORAGE_OBJECT_HPP
#define LLFS_SIMULATED_STORAGE_OBJECT_HPP

#include <llfs/config.hpp>
//
#include <llfs/int_types.hpp>

namespace llfs {

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
//
class SimulatedStorageObject
{
 public:
  SimulatedStorageObject(const SimulatedStorageObject&) = delete;
  SimulatedStorageObject& operator=(const SimulatedStorageObject&) = delete;

  virtual ~SimulatedStorageObject() = default;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  virtual void crash_and_recover(u64 simulation_step) = 0;

 protected:
  SimulatedStorageObject() = default;
};

}  //namespace llfs

#endif  // LLFS_SIMULATED_STORAGE_OBJECT_HPP
