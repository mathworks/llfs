//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#define LLFS_PAGE_CACHE_OVERCOMMIT_HPP

#include <llfs/config.hpp>
//

namespace llfs {

/** \brief Controls whether a given cache insertion is allowed to exceed the cache size limit.
 *
 * In certain cases, allowing a cache over-commit is preferable to deadlock.  This mechanism
 * allows the application to specify whether over-commit will be allowed for a given cache
 * operation, and if so, whether the over-commit was triggered.
 */
class PageCacheOvercommit
{
 public:
  using Self = PageCacheOvercommit;

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  static Self& not_allowed()
  {
    static Self obj_;
    return obj_;
  }

  //+++++++++++-+-+--+----- --- -- -  -  -   -

  PageCacheOvercommit() = default;

  PageCacheOvercommit(const PageCacheOvercommit&) = delete;
  PageCacheOvercommit& operator=(const PageCacheOvercommit&) = delete;

  bool is_allowed() const
  {
    return this->allowed_;
  }

  bool is_triggered() const
  {
    return this->triggered_;
  }

  void allow(bool b = true)
  {
    this->allowed_ = b;
  }

  void trigger(bool b = true)
  {
    this->triggered_ = b;
  }

 private:
  bool allowed_ = false;
  bool triggered_ = false;
};

}  //namespace llfs
