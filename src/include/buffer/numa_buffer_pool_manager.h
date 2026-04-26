//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// numa_buffer_pool_manager.h
//
// Identification: src/include/buffer/numa_buffer_pool_manager.h
//
// NUMA-aware extension of the BusTub buffer pool manager.
//
// Two allocation policies are supported:
//   INTERLEAVE – pages are distributed round-robin across all NUMA nodes via
//                mbind(MPOL_INTERLEAVE).  Good for multi-threaded workloads
//                where threads run on different nodes.
//   BIND       – all pages are allocated on a single NUMA node via
//                mbind(MPOL_BIND).  Minimises remote-access latency when the
//                workload is pinned to one node, but causes cross-node traffic
//                when threads on the other node access the pool.
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <vector>

#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

/** Allocation policy for the NUMA-aware buffer pool. */
enum class NumaPolicy {
  INTERLEAVE,  ///< Distribute frames across all NUMA nodes (mbind MPOL_INTERLEAVE)
  BIND,        ///< Pin all frames to a single NUMA node   (mbind MPOL_BIND)
};

/**
 * @brief NUMA-aware frame header.
 *
 * Unlike the stock FrameHeader whose data_ is a heap-allocated std::vector,
 * this variant points into a large mmap region whose NUMA policy has been set
 * with mbind().  The pointer is non-owning; lifetime is managed by
 * NumaBufferPoolManager.
 */
class NumaFrameHeader {
  friend class NumaBufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  /** @param frame_id  Index of this frame.
   *  @param data      Pointer into the mmap arena for this frame's 8 KiB. */
  NumaFrameHeader(frame_id_t frame_id, char *data);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();

  const frame_id_t frame_id_;
  std::shared_mutex rwlatch_;
  std::atomic<size_t> pin_count_{0};
  bool is_dirty_{false};
  page_id_t page_id_{INVALID_PAGE_ID};

  /** Non-owning pointer into the mmap arena. */
  char *data_;
};

/**
 * @brief NUMA-aware buffer pool manager.
 *
 * Allocates the entire frame arena with a single mmap() call and then applies
 * an mbind() policy so the kernel places physical pages according to the
 * chosen NumaPolicy.
 *
 * The rest of the logic (page table, replacer, disk scheduler, page guards) is
 * identical to the stock BufferPoolManager so the two can be compared fairly.
 */
class NumaBufferPoolManager {
 public:
  /**
   * @param num_frames  Number of frames in the pool.
   * @param disk_manager  Disk manager (shared with the rest of the system).
   * @param policy  NUMA allocation policy.
   * @param bind_node  NUMA node to bind to when policy == BIND.  Ignored for
   *                   INTERLEAVE.  Defaults to node 0.
   * @param log_manager  Ignored (kept for API symmetry).
   */
  NumaBufferPoolManager(size_t num_frames, DiskManager *disk_manager, NumaPolicy policy, int bind_node = 0,
                        LogManager *log_manager = nullptr);

  ~NumaBufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;

  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<ReadPageGuard>;

  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;

  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;

  /** @brief Return the policy this instance was constructed with. */
  auto Policy() const -> NumaPolicy { return policy_; }

 private:
  /**
   * @brief Allocate the mmap arena and apply the requested mbind policy.
   * @return Pointer to the start of the arena, or MAP_FAILED on error.
   */
  auto AllocateArena() -> char *;

  /**
   * @brief Core logic shared by CheckedWritePage / CheckedReadPage.
   *
   * Finds or loads the requested page into a frame and returns the frame
   * pointer with pin_count incremented.  Caller must release bpm_latch_
   * before constructing the page guard.
   */
  auto FetchFrame(page_id_t page_id, AccessType access_type, std::unique_lock<std::mutex> &lk)
      -> std::shared_ptr<NumaFrameHeader>;

  // ── configuration ────────────────────────────────────────────────────────
  const size_t num_frames_;
  const NumaPolicy policy_;
  const int bind_node_;

  // ── mmap arena ───────────────────────────────────────────────────────────
  char *arena_{nullptr};   ///< Base of the mmap region
  size_t arena_size_{0};   ///< Total size in bytes

  // ── BPM internals (mirrors stock BufferPoolManager) ──────────────────────
  std::atomic<page_id_t> next_page_id_{0};
  std::shared_ptr<std::mutex> bpm_latch_;
  std::vector<std::shared_ptr<NumaFrameHeader>> frames_;
  std::unordered_map<page_id_t, frame_id_t> page_table_;
  std::list<frame_id_t> free_frames_;
  std::shared_ptr<ArcReplacer> replacer_;
  std::shared_ptr<DiskScheduler> disk_scheduler_;
};

}  // namespace bustub
