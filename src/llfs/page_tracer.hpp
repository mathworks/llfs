//#=##=##=#==#=#==#===#+==#+==========+==+=+=+=+=+=++=+++=+++++=-++++=-+++++++++++
//
// Part of the LLFS Project, under Apache License v2.0.
// See https://www.apache.org/licenses/LICENSE-2.0 for license information.
// SPDX short identifier: Apache-2.0
//
//+++++++++++-+-+--+----- --- -- -  -  -   -

#pragma once
#ifndef LLFS_PAGE_TRACER_HPP
#define LLFS_PAGE_TRACER_HPP

#include <llfs/page_loader.hpp>
#include <llfs/pinned_page.hpp>
#include <llfs/ref.hpp>

#include <batteries/seq.hpp>
#include <batteries/status.hpp>

namespace llfs {
//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief An interface representing an entity that has the capability to trace and/or store data
 * regarding the outgoing reference counts of pages.
 */
class PageTracer
{
 public:
  PageTracer(const PageTracer&) = delete;
  PageTracer& operator=(const PageTracer&) = delete;

  virtual ~PageTracer() = default;

  virtual batt::StatusOr<batt::BoxedSeq<PageId>> trace_page_refs(PageId from_page_id) noexcept = 0;

 protected:
  PageTracer() = default;
};

//=#=#==#==#===============+=+=+=+=++=++++++++++++++-++-+--+-+----+---------------
/** \brief A PageTracer with the ability to load a page and trace its outgoing references to other
 * pages.
 */
class LoadingPageTracer : public PageTracer
{
 public:
  explicit LoadingPageTracer(PageLoader& page_loader) noexcept;

  ~LoadingPageTracer();

  LoadingPageTracer(const LoadingPageTracer&) = delete;
  LoadingPageTracer& operator=(const LoadingPageTracer&) = delete;

  /** \brief Loads the page with page id `from_page_id` and traces its outgoing references.
   * \return A sequence of page ids for the pages referenced from the page `from_page_id`.
   */
  batt::StatusOr<batt::BoxedSeq<PageId>> trace_page_refs(PageId from_page_id) noexcept override;

 private:
  /** \brief The PageLoader used to load the page being traced.
   */
  Ref<PageLoader> page_loader_;
};

}  // namespace llfs

#endif  // LLFS_PAGE_TRACER_HPP
