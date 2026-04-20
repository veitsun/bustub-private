//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <utility>
#include <vector>
#include "buffer/arc_replacer.h"
#include "common/macros.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 对 frame_-> rwlatch_ 这把 shared_mutex 加一个读锁
  // 然后把锁的拥有权交给这个 shared_lock 对象管理
  page_lock_ = std::shared_lock<std::shared_mutex>(frame_->rwlatch_); // page_lock_ 用来保存一个共享锁对象
  is_valid_ = true;

}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // 移动构造

  // 转移所有字段
  page_id_    = that.page_id_;
  frame_      = std::move(that.frame_);
  replacer_   = std::move(that.replacer_);
  bpm_latch_  = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  page_lock_  = std::move(that.page_lock_); // 锁的所有权也要转移
  is_valid_   = that.is_valid_;

  // 让 that 失效，防止它析构时 double free
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if(this == &that) { return *this; }

  Drop();

  // 先释放自己，再接管 that
  page_id_    = that.page_id_;
  frame_      = std::move(that.frame_);
  replacer_   = std::move(that.replacer_);
  bpm_latch_  = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  page_lock_  = std::move(that.page_lock_); // 锁的所有权也要转移
  is_valid_   = that.is_valid_;

  that.is_valid_ = false;
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 将脏页写回磁盘，需要用 disk_scheduler_ 发起一个写请求：
  // ReadPageGuard 持有的是共享读锁，理论上读操作不会修改数据，所以页面不应该是脏的。

}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  if (!is_valid_) {
    // 防止 double free
    return ;
  }
  is_valid_ = false;

  // 释放页面锁
  page_lock_.unlock();

  // 将 frame 的 pin_count 减 1 ， 并在 pin_count 降为 0 时。通知 replacer 该 frame 可以被淘汰
  {
    // 先释放页面锁， 再拿 bpm_latch 修改 pin_count ，顺序反了会死锁
    std::lock_guard<std::mutex> lk(*bpm_latch_);
    frame_->pin_count_ --;
    if(frame_->pin_count_ == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  page_lock_ = std::unique_lock<std::shared_mutex>(frame_->rwlatch_);
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // 移动构造

  // 转移所有字段
  page_id_    = that.page_id_;
  frame_      = std::move(that.frame_);
  replacer_   = std::move(that.replacer_);
  bpm_latch_  = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  page_lock_  = std::move(that.page_lock_); // 锁的所有权也要转移
  is_valid_   = that.is_valid_;

  // 让 that 失效，防止它析构时 double free
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard; otherwise, you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if(this == &that) { return *this; }

  Drop();

  // 先释放自己，再接管 that
  page_id_    = that.page_id_;
  frame_      = std::move(that.frame_);
  replacer_   = std::move(that.replacer_);
  bpm_latch_  = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  page_lock_  = std::move(that.page_lock_); // 锁的所有权也要转移
  is_valid_   = that.is_valid_;

  that.is_valid_ = false;

  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true; // 只要调用了 GetDataMut 就说明页面被修改了，设置脏页标记
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 将脏页写回磁盘，需要用 disk_scheduler_ 发起一个写请求：
  // WritePageGuard 持有独占写锁，有完整的写权限，flush 之后可以安全地把 is_dirty_ 置为false，因为此时没有其他线程能修改这个页面。
  // std::lock_guard<std::mutex> lk(*bpm_latch_);
  if(!is_valid_ || !frame_->is_dirty_)  {return ; }
  frame_->is_dirty_ = false;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  
  std::vector<DiskRequest> requests;
  requests.push_back({true, frame_->GetDataMut(), page_id_, std::move(promise)});
  disk_scheduler_->Schedule(requests);
  future.get();
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
    if (!is_valid_) {
    // 防止 double free
    return ;
  }
  is_valid_ = false;

  // 释放页面锁
  page_lock_.unlock();

  // 将 frame 的 pin_count 减 1 ， 并在 pin_count 降为 0 时。通知 replacer 该 frame 可以被淘汰
  {
    // 先释放页面锁， 再拿 bpm_latch 修改 pin_count ，顺序反了会死锁
    std::lock_guard<std::mutex> lk(*bpm_latch_);
    frame_->pin_count_ --;
    if(frame_->pin_count_ == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
