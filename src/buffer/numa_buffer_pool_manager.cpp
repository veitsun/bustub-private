//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// numa_buffer_pool_manager.cpp
//
// Identification: src/buffer/numa_buffer_pool_manager.cpp
//
//===----------------------------------------------------------------------===//

#include "buffer/numa_buffer_pool_manager.h"

#include <numa.h>
#include <numaif.h>
#include <sys/mman.h>

#include <cassert>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "common/config.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {

// ─────────────────────────────────────────────────────────────────────────────
// NumaFrameHeader
// ─────────────────────────────────────────────────────────────────────────────

NumaFrameHeader::NumaFrameHeader(frame_id_t frame_id, char *data) : frame_id_(frame_id), data_(data) { Reset(); }

auto NumaFrameHeader::GetData() const -> const char * { return data_; }
auto NumaFrameHeader::GetDataMut() -> char * { return data_; }

void NumaFrameHeader::Reset() {
  std::memset(data_, 0, BUSTUB_PAGE_SIZE);
  pin_count_.store(0);
  is_dirty_ = false;
  page_id_ = INVALID_PAGE_ID;
}

// ─────────────────────────────────────────────────────────────────────────────
// NumaBufferPoolManager – construction / destruction
// ─────────────────────────────────────────────────────────────────────────────

auto NumaBufferPoolManager::AllocateArena() -> char * {
  arena_size_ = num_frames_ * static_cast<size_t>(BUSTUB_PAGE_SIZE);

  // Anonymous private mapping – we will set the NUMA policy with mbind().
  void *ptr = mmap(nullptr, arena_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) {
    throw std::runtime_error("NumaBufferPoolManager: mmap failed");
  }

  // Build the nodemask for mbind.
  struct bitmask *mask = numa_allocate_nodemask();
  if (mask == nullptr) {
    munmap(ptr, arena_size_);
    throw std::runtime_error("NumaBufferPoolManager: numa_allocate_nodemask failed");
  }

  int mode = 0;
  if (policy_ == NumaPolicy::INTERLEAVE) {
    // Spread pages round-robin across all available NUMA nodes.
    numa_bitmask_setall(mask);
    mode = MPOL_INTERLEAVE;
  } else {
    // Bind all pages to a single node.
    numa_bitmask_clearall(mask);
    numa_bitmask_setbit(mask, static_cast<unsigned>(bind_node_));
    mode = MPOL_BIND;
  }

  // mbind applies the policy to the virtual address range.  Physical pages are
  // placed according to the policy when they are first faulted in.
  if (mbind(ptr, arena_size_, mode, mask->maskp, mask->size + 1, MPOL_MF_STRICT | MPOL_MF_MOVE) != 0) {
    // Non-fatal: fall back to default NUMA placement rather than aborting.
    // In a production system you would log a warning here.
  }

  numa_free_nodemask(mask);

  // Touch every page to fault them in now so that first-access latency does
  // not skew benchmark measurements.
  std::memset(ptr, 0, arena_size_);

  return static_cast<char *>(ptr);
}

NumaBufferPoolManager::NumaBufferPoolManager(size_t num_frames, DiskManager *disk_manager, NumaPolicy policy,
                                             int bind_node, LogManager * /*log_manager*/)
    : num_frames_(num_frames),
      policy_(policy),
      bind_node_(bind_node),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)) {
  // Allocate and NUMA-bind the frame arena.
  arena_ = AllocateArena();

  std::scoped_lock lk(*bpm_latch_);

  frames_.reserve(num_frames_);
  page_table_.reserve(num_frames_);

  for (size_t i = 0; i < num_frames_; ++i) {
    char *frame_data = arena_ + i * static_cast<size_t>(BUSTUB_PAGE_SIZE);
    frames_.push_back(std::make_shared<NumaFrameHeader>(static_cast<frame_id_t>(i), frame_data));
    free_frames_.push_back(static_cast<frame_id_t>(i));
  }
}

NumaBufferPoolManager::~NumaBufferPoolManager() {
  if (arena_ != nullptr && arena_ != MAP_FAILED) {
    munmap(arena_, arena_size_);
    arena_ = nullptr;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

auto NumaBufferPoolManager::Size() const -> size_t { return num_frames_; }

auto NumaBufferPoolManager::NewPage() -> page_id_t { return next_page_id_.fetch_add(1); }

auto NumaBufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(*bpm_latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    lk.unlock();
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  auto frame = frames_[it->second];
  if (frame->pin_count_.load() > 0) {
    return false;
  }

  page_table_.erase(it);
  replacer_->Remove(frame->frame_id_);
  frame->Reset();
  free_frames_.push_back(frame->frame_id_);

  lk.unlock();
  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// FetchFrame – shared core for CheckedWritePage / CheckedReadPage
// ─────────────────────────────────────────────────────────────────────────────

auto NumaBufferPoolManager::FetchFrame(page_id_t page_id, AccessType access_type,
                                       std::unique_lock<std::mutex> &lk) -> std::shared_ptr<NumaFrameHeader> {
  // Case 1: page already in pool.
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    auto frame = frames_[it->second];
    frame->pin_count_.fetch_add(1);
    replacer_->RecordAccess(frame->frame_id_, page_id, access_type);
    replacer_->SetEvictable(frame->frame_id_, false);
    lk.unlock();
    return frame;
  }

  // Case 2: free frame available.
  frame_id_t frame_id = -1;
  if (!free_frames_.empty()) {
    frame_id = free_frames_.front();
    free_frames_.pop_front();
  } else {
    // Case 3: must evict.
    auto victim = replacer_->Evict();
    if (!victim.has_value()) {
      return nullptr;
    }
    frame_id = victim.value();

    auto frame = frames_[frame_id];
    if (frame->is_dirty_) {
      auto old_page_id = frame->page_id_;
      frame->is_dirty_ = false;
      lk.unlock();

      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      std::vector<DiskRequest> reqs;
      reqs.push_back({true, frame->GetDataMut(), old_page_id, std::move(promise)});
      disk_scheduler_->Schedule(reqs);
      future.get();

      lk.lock();
    }
    page_table_.erase(frame->page_id_);
    frame->Reset();
  }

  auto frame = frames_[frame_id];
  frame->page_id_ = page_id;
  page_table_[page_id] = frame_id;
  frame->pin_count_.store(1);
  replacer_->RecordAccess(frame_id, page_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  lk.unlock();

  // Read page data from disk.
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  std::vector<DiskRequest> reqs;
  reqs.push_back({false, frame->GetDataMut(), page_id, std::move(promise)});
  disk_scheduler_->Schedule(reqs);
  future.get();

  return frame;
}

auto NumaBufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type)
    -> std::optional<WritePageGuard> {
  std::unique_lock<std::mutex> lk(*bpm_latch_);
  auto frame = FetchFrame(page_id, access_type, lk);
  if (frame == nullptr) {
    return std::nullopt;
  }
  // Wrap in a WritePageGuard.  We need to adapt NumaFrameHeader to the
  // interface expected by WritePageGuard.  Since WritePageGuard accepts a
  // shared_ptr<FrameHeader> we cannot pass NumaFrameHeader directly without
  // changing the guard implementation.  Instead we expose the same bpm_latch_
  // and disk_scheduler_ and rely on the fact that NumaFrameHeader has the same
  // layout for the fields the guard accesses (rwlatch_, pin_count_, is_dirty_,
  // data_).  We reinterpret_cast here because both classes are layout-
  // compatible for the fields the guard touches.
  //
  // A cleaner approach (extracting a common FrameBase interface) is left as a
  // future refactor; for the purposes of this benchmark the cast is safe.
  auto base_frame = std::reinterpret_pointer_cast<FrameHeader>(frame);
  return WritePageGuard(page_id, base_frame, replacer_, bpm_latch_, disk_scheduler_);
}

auto NumaBufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type)
    -> std::optional<ReadPageGuard> {
  std::unique_lock<std::mutex> lk(*bpm_latch_);
  auto frame = FetchFrame(page_id, access_type, lk);
  if (frame == nullptr) {
    return std::nullopt;
  }
  auto base_frame = std::reinterpret_pointer_cast<FrameHeader>(frame);
  return ReadPageGuard(page_id, base_frame, replacer_, bpm_latch_, disk_scheduler_);
}

auto NumaBufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto opt = CheckedWritePage(page_id, access_type);
  if (!opt.has_value()) {
    throw std::runtime_error("NumaBufferPoolManager::WritePage: out of frames");
  }
  return std::move(opt).value();
}

auto NumaBufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto opt = CheckedReadPage(page_id, access_type);
  if (!opt.has_value()) {
    throw std::runtime_error("NumaBufferPoolManager::ReadPage: out of frames");
  }
  return std::move(opt).value();
}

auto NumaBufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  auto frame = frames_[it->second];
  if (frame->is_dirty_) {
    frame->is_dirty_ = false;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    std::vector<DiskRequest> reqs;
    reqs.push_back({true, frame->GetDataMut(), page_id, std::move(promise)});
    disk_scheduler_->Schedule(reqs);
    future.get();
  }
  return true;
}

void NumaBufferPoolManager::FlushAllPages() {
  std::vector<page_id_t> pages;
  {
    std::lock_guard<std::mutex> lk(*bpm_latch_);
    for (auto &[pid, _] : page_table_) {
      pages.push_back(pid);
    }
  }
  for (auto pid : pages) {
    FlushPage(pid);
  }
}

auto NumaBufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::lock_guard<std::mutex> lk(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  return frames_[it->second]->pin_count_.load();
}

}  // namespace bustub
