//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param requests The requests to be scheduled.
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  // 把 requests 这个请求列表放到 请求队列里面，进行后续调度
  for(auto &request : requests) {
    request_queue_.Put(std::make_optional(std::move(request))); // 如果不使用移动语义，意味着每个 DiskRequest 对象都会被完整拷贝一份到队列中
    // 如果 DiskRequest 内部有大数组，字符串或指针等资源，拷贝开销会很大
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  // 开始处理请求队列
  while (true) {
    // 拿到单个请求
    auto request_opt = request_queue_.Get();
    //
    if(!request_opt.has_value()) {return ;}

    auto request = std::move(request_opt.value());

    if(request.is_write_) {
      disk_manager_->WritePage(request.page_id_, request.data_);
    }
    else {
      disk_manager_->ReadPage(request.page_id_, request.data_);
    }

    request.callback_.set_value(true);
  }
}

}  // namespace bustub
