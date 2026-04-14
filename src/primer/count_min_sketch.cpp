//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// count_min_sketch.cpp
//
// Identification: src/primer/count_min_sketch.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/count_min_sketch.h"

#include <stdexcept>
#include <string>

namespace bustub {

/**
 * Constructor for the count-min sketch.
 *
 * @param width The width of the sketch matrix.
 * @param depth The depth of the sketch matrix.
 * @throws std::invalid_argument if width or depth are zero.
 */
template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(uint32_t width, uint32_t depth) : width_(width), depth_(depth) {
  /** @TODO(student) Implement this function! */
  if (width == 0 || depth == 0) {
    throw std::invalid_argument("width and depth must be > 0");
  }
  
  sketch_ = std::make_unique<std::atomic<uint32_t>[]>(width_ * depth_);

  for (size_t i = 0; i < width_ * depth_; i++) {
    sketch_[i].store(0, std::memory_order_relaxed);
  }

  /** @fall2025 PLEASE DO NOT MODIFY THE FOLLOWING */
  // Initialize seeded hash functions
  hash_functions_.reserve(depth_);
  for (size_t i = 0; i < depth_; i++) {
    hash_functions_.push_back(this->HashFunction(i));
  }
}

template <typename KeyType>
CountMinSketch<KeyType>::CountMinSketch(CountMinSketch &&other) noexcept
    : width_(other.width_),
      depth_(other.depth_),
      hash_functions_(std::move(other.hash_functions_)),
      sketch_(std::move(other.sketch_)) {}

template <typename KeyType>
auto CountMinSketch<KeyType>::operator=(CountMinSketch &&other) noexcept -> CountMinSketch & {
  if (this != &other) {
    width_ = other.width_;
    depth_ = other.depth_;
    hash_functions_ = std::move(other.hash_functions_);
    sketch_ = std::move(other.sketch_);
  }
  return *this;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Insert(const KeyType &item) {
  /** @TODO(student) Implement this function! */
  //aquire lock
  for(size_t i = 0; i < depth_; i++){
    size_t col = hash_functions_[i](item);
    sketch_[Pos(i, col)].fetch_add(1, std::memory_order_relaxed);
  }
}

template <typename KeyType>
void CountMinSketch<KeyType>::Merge(const CountMinSketch<KeyType> &other) {
  if (width_ != other.width_ || depth_ != other.depth_) {
    throw std::invalid_argument("Incompatible CountMinSketch dimensions for merge.");
  }
  /** @TODO(student) Implement this function! */
  size_t total = width_ * depth_;
  for (size_t i = 0; i < total; i++) {
    sketch_[i].fetch_add(other.sketch_[i].load(std::memory_order_relaxed),
                         std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::Count(const KeyType &item) const -> uint32_t {
  uint32_t res = UINT32_MAX;
  for (size_t i = 0; i < depth_; i++){
    size_t col = hash_functions_[i](item);
    uint32_t v = sketch_[Pos(i, col)].load(std::memory_order_relaxed);
    res = std::min(res, v);
  }
  return res == UINT32_MAX ? 0 : res;
}

template <typename KeyType>
void CountMinSketch<KeyType>::Clear() {
  /** @TODO(student) Implement this function! */
  for (size_t i = 0; i < width_ * depth_; i++) {
    sketch_[i].store(0, std::memory_order_relaxed);
  }
}

template <typename KeyType>
auto CountMinSketch<KeyType>::TopK(uint16_t k, const std::vector<KeyType> &candidates)
    -> std::vector<std::pair<KeyType, uint32_t>> {
  /** @TODO(student) Implement this function! */
  if (k == 0) return {};
  std::priority_queue<
    std::pair<uint32_t, KeyType>, 
    std::vector<std::pair<uint32_t, KeyType>>,
    std::greater<>
    > min_heap_;
  for(const auto& item : candidates){
    size_t c = Count(item);
    min_heap_.emplace(c, item);
    if (min_heap_.size() > k){
      min_heap_.pop();
    }
  }
  std::vector<std::pair<KeyType, uint32_t>> res;
  while(!min_heap_.empty()){
    auto [cnt, key] = min_heap_.top();
    res.emplace_back(std::make_pair(key, cnt));
    min_heap_.pop();
  }
  std::reverse(res.begin(), res.end());
  return res;
}

// Explicit instantiations for all types used in tests
template class CountMinSketch<std::string>;
template class CountMinSketch<int64_t>;  // For int64_t tests
template class CountMinSketch<int>;      // This covers both int and int32_t
}  // namespace bustub
