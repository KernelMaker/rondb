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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_METRICS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_METRICS_HPP_

#include <ndb_types.h>
#include <NdbTick.h>
#include <string>
#include <vector>
#include <mutex>
#include <chrono>

// Configuration - can be changed at compile time or made runtime configurable later
extern bool g_scan_timing_enabled;
extern Uint64 g_slow_scan_threshold_us;
extern Uint32 g_slow_scan_buffer_size;

// Timing for each scan phase (in microseconds)
struct ScanPhaseTiming {
  Uint64 json_parse_us = 0;           // Step 1: json.scan_parse
  Uint64 validation_us = 0;           // Step 2: validation (db/table/columns/filter/index)
  Uint64 preparation_us = 0;          // Step 3.1: setup before startTransaction
  Uint64 start_transaction_us = 0;    // Step 3.2: startTransaction()
  Uint64 compile_filter_us = 0;       // Step 3.3: BindFilterColumns + CompileFilter
  Uint64 scan_index_setup_us = 0;     // Step 3.4: scanIndex() or scanTable()
  Uint64 compile_index_range_us = 0;  // Step 3.5: CompileIndexRanges
  Uint64 execute_us = 0;              // Step 3.6: transaction->execute()
  Uint64 next_result_us = 0;          // Step 3.7a: sum of nextResult() calls
  Uint64 json_serialize_us = 0;       // Step 3.7b: sum of WriteColumnData2Json()
  Uint64 callback_us = 0;             // Step 4: callback()
  Uint64 total_us = 0;                // Total operation time

  // Context
  Uint64 rows_fetched = 0;
  Uint64 limit = 0;
  std::string database;
  std::string table;
  std::string index_name;
  bool has_filter = false;
  bool is_index_scan = false;
};

// Slow scan entry for the circular buffer
struct SlowScanEntry {
  ScanPhaseTiming timing;
  Uint64 timestamp_ms;    // Unix timestamp in milliseconds
  Uint32 thread_id;
};

// Thread-safe circular buffer for slow queries
class SlowScanBuffer {
public:
  explicit SlowScanBuffer(size_t capacity);
  ~SlowScanBuffer();

  // Add entry to buffer (thread-safe)
  void add(const SlowScanEntry& entry);

  // Get all entries (thread-safe, returns copy)
  std::vector<SlowScanEntry> getAll() const;

  // Clear all entries (thread-safe)
  void clear();

  // Get current count in buffer
  size_t count() const;

  // Get total count since last reset (includes overwritten entries)
  Uint64 totalCount() const;

private:
  mutable std::mutex mutex_;
  std::vector<SlowScanEntry> buffer_;
  size_t capacity_;
  size_t head_ = 0;
  size_t count_ = 0;
  Uint64 total_count_ = 0;  // Total slow scans since last reset
};

// Global slow scan buffer
extern SlowScanBuffer* g_slow_scan_buffer;

// Initialize scan metrics (call at startup)
void initScanMetrics();

// Cleanup scan metrics (call at shutdown)
void cleanupScanMetrics();

// Record slow scan if exceeds threshold
void maybeRecordSlowScan(const ScanPhaseTiming& timing, Uint32 thread_id);

// Get current timestamp in milliseconds
inline Uint64 getCurrentTimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::system_clock::now().time_since_epoch()).count();
}

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_SCAN_METRICS_HPP_
