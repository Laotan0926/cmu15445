//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  if(lru_list_.empty()){
    *frame_id = INVALID_PAGE_ID;
    return false;
  }
  *frame_id = lru_list_.back();
  lru_list_.pop_back();
  lru_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto iterator = lru_map_.find(frame_id);
  if(iterator!=lru_map_.end()){
    lru_list_.erase(iterator->second);
    lru_map_.erase(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto iterator = lru_map_.find(frame_id);
  if(iterator==lru_map_.end()){
    lru_list_.push_front(frame_id);
    lru_map_.emplace(frame_id,lru_list_.begin());
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(mutex_);
  return lru_list_.size();
}

}  // namespace bustub
