/*
 * Copyright (C) 2024, 2025 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "scan_metrics.hpp"

// Global configuration - defaults
bool g_scan_timing_enabled = true;
Uint64 g_slow_scan_threshold_us = 10000;  // 10ms default
Uint32 g_slow_scan_buffer_size = 1000;

// Global buffer
SlowScanBuffer* g_slow_scan_buffer = nullptr;

// SlowScanBuffer implementation
SlowScanBuffer::SlowScanBuffer(size_t capacity)
    : capacity_(capacity), head_(0), count_(0) {
  buffer_.resize(capacity);
}

SlowScanBuffer::~SlowScanBuffer() {
  // Nothing to do - vector cleans up itself
}

void SlowScanBuffer::add(const SlowScanEntry& entry) {
  std::lock_guard<std::mutex> lock(mutex_);
  buffer_[head_] = entry;
  head_ = (head_ + 1) % capacity_;
  if (count_ < capacity_) {
    count_++;
  }
  total_count_++;
}

std::vector<SlowScanEntry> SlowScanBuffer::getAll() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<SlowScanEntry> result;
  result.reserve(count_);

  // Return entries in order from oldest to newest
  if (count_ < capacity_) {
    // Buffer not full yet - entries are from 0 to count_-1
    for (size_t i = 0; i < count_; i++) {
      result.push_back(buffer_[i]);
    }
  } else {
    // Buffer is full - oldest entry is at head_
    for (size_t i = 0; i < count_; i++) {
      result.push_back(buffer_[(head_ + i) % capacity_]);
    }
  }
  return result;
}

void SlowScanBuffer::clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  head_ = 0;
  count_ = 0;
  total_count_ = 0;
}

size_t SlowScanBuffer::count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return count_;
}

Uint64 SlowScanBuffer::totalCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return total_count_;
}

void initScanMetrics() {
  if (g_slow_scan_buffer == nullptr) {
    g_slow_scan_buffer = new SlowScanBuffer(g_slow_scan_buffer_size);
  }
}

void cleanupScanMetrics() {
  if (g_slow_scan_buffer != nullptr) {
    delete g_slow_scan_buffer;
    g_slow_scan_buffer = nullptr;
  }
}

void maybeRecordSlowScan(const ScanPhaseTiming& timing, Uint32 thread_id) {
  // Skip if timing disabled or buffer not initialized
  if (!g_scan_timing_enabled || g_slow_scan_buffer == nullptr) {
    return;
  }

  // Only record if exceeds threshold
  if (timing.total_us >= g_slow_scan_threshold_us) {
    SlowScanEntry entry;
    entry.timing = timing;
    entry.timestamp_ms = getCurrentTimestampMs();
    entry.thread_id = thread_id;
    g_slow_scan_buffer->add(entry);
  }
}
