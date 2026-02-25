/*
 * bench_pa_qps.cpp — NDB Pushdown Aggregation vs MySQL QPS Benchmark
 *
 * Bypasses RDRS/HTTP entirely. Uses the NDB C++ API (NdbAggregator +
 * NdbScanOperation::DoAggregation) directly, compared with MySQL C API
 * queries on the same tables.
 *
 * Build (from the RonDB build directory):
 *   cd storage/ndb/ndbapi-examples
 *   # Already added to CMakeLists.txt, or compile standalone:
 *   g++ -std=c++17 -O2 -o bench_pa_qps bench_pa_qps.cpp \
 *       -I<rondb>/include -I<rondb>/storage/ndb/include \
 *       -L<rondb-bin>/lib -lndbclient -lmysqlclient -lpthread
 *
 * Usage:
 *   ./bench_pa_qps -c <ndb_connectstring> -h <mysql_host> [-d <database>]
 *                  [--concurrency N] [--duration N] [--tiers 100,500,10000]
 */

#ifdef _WIN32
#include <winsock2.h>
#endif
#include <mysql.h>
#include <NdbApi.hpp>
#include <NdbSleep.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
static const char *g_ndb_connectstring = "localhost:1186";
static const char *g_mysql_host = "127.0.0.1";
static int         g_mysql_port = 3306;
static const char *g_mysql_user = "root";
static const char *g_mysql_pass = "";
static const char *g_database   = "bench_pa_qps";

static int g_concurrency = 32;
static int g_duration    = 60;   // seconds
static int g_warmup      = 5;    // seconds
static std::vector<int> g_tiers = {100, 500, 10000, 100000, 500000};
static int g_batch_size  = 90000;
static int g_ndb_connections = 4; // number of NDB cluster connections (match mysqld)
static bool g_validate = false;   // validate PA results against MySQL before benchmarking

// Query definitions — each is a (name, programming_fn) pair
struct QueryDef {
  const char *name;
  // Programs the aggregator for a given table.  Returns number of agg results.
  int (*program)(NdbAggregator &agg);
  // Equivalent SQL (with %s for table name)
  const char *sql_fmt;
};

// Q1: COUNT(*), SUM(val1), AVG(val3) — AVG = SUM/COUNT
static int program_q1(NdbAggregator &agg) {
  agg.LoadUint64(1, kReg1);
  agg.Count(0, kReg1);          // agg0 = COUNT(*)
  agg.LoadColumn("val1", kReg1);
  agg.Sum(1, kReg1);            // agg1 = SUM(val1)
  agg.LoadColumn("val3", kReg1);
  agg.Sum(2, kReg1);            // agg2 = SUM(val3)  (for AVG numerator)
  agg.Count(3, kReg1);          // agg3 = COUNT(val3) (for AVG denominator)
  return 4;
}

// Q2: COUNT(*), SUM(val1), SUM(val2), AVG(val3), MIN(val4), MAX(val4)
static int program_q2(NdbAggregator &agg) {
  agg.LoadUint64(1, kReg1);
  agg.Count(0, kReg1);
  agg.LoadColumn("val1", kReg1);
  agg.Sum(1, kReg1);
  agg.LoadColumn("val2", kReg1);
  agg.Sum(2, kReg1);
  agg.LoadColumn("val3", kReg1);
  agg.Sum(3, kReg1);
  agg.Count(4, kReg1);
  agg.LoadColumn("val4", kReg1);
  agg.Min(5, kReg1);
  agg.Max(6, kReg1);
  return 7;
}

// Q3: COUNT(*), SUM(val1 * val3), AVG(val3 + val4)
static int program_q3(NdbAggregator &agg) {
  agg.LoadUint64(1, kReg1);
  agg.Count(0, kReg1);
  agg.LoadColumn("val1", kReg1);
  agg.LoadColumn("val3", kReg2);
  agg.Mul(kReg1, kReg2);
  agg.Sum(1, kReg1);            // SUM(val1*val3)
  agg.LoadColumn("val3", kReg1);
  agg.LoadColumn("val4", kReg2);
  agg.Add(kReg1, kReg2);
  agg.Sum(2, kReg1);            // SUM(val3+val4) for AVG
  agg.Count(3, kReg1);          // COUNT(val3+val4) for AVG
  return 4;
}

// Q4: COUNT(*), SUM(val1), AVG(val3) with filter (filter_date >= '2024-07-01')
// Same aggregation program as Q1; filter applied via index scan on idx_date
static int program_q4(NdbAggregator &agg) {
  return program_q1(agg);
}

static QueryDef g_queries[] = {
  {"Q1_simple",
   program_q1,
   "SELECT COUNT(*), SUM(val1), SUM(val3), COUNT(val3) FROM %s"},
  {"Q2_multi_agg",
   program_q2,
   "SELECT COUNT(*), SUM(val1), SUM(val2), SUM(val3), COUNT(val3), MIN(val4), MAX(val4) FROM %s"},
  {"Q3_expr",
   program_q3,
   "SELECT COUNT(*), SUM(val1*val3), SUM(val3+val4), COUNT(val3+val4) FROM %s"},
  {"Q4_filtered",
   program_q4,
   "SELECT COUNT(*), SUM(val1), SUM(val3), COUNT(val3) FROM %s WHERE filter_date >= '2024-07-01'"},
};
static const int NUM_QUERIES = sizeof(g_queries) / sizeof(g_queries[0]);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
static std::string table_name(int rows) {
  return "bench_qps_" + std::to_string(rows);
}

static void die(const char *msg) {
  fprintf(stderr, "FATAL: %s\n", msg);
  exit(1);
}

// Timestamp in seconds (monotonic)
static double now_sec() {
  using namespace std::chrono;
  return duration<double>(steady_clock::now().time_since_epoch()).count();
}

// Timestamp in nanoseconds (for latency)
static int64_t now_ns() {
  using namespace std::chrono;
  return duration_cast<nanoseconds>(
      steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Latency collector (thread-safe)
// ---------------------------------------------------------------------------
class LatencyCollector {
 public:
  void reserve(size_t n) {
    std::lock_guard<std::mutex> lk(mu_);
    samples_.reserve(n);
  }
  void add(int64_t latency_us) {
    std::lock_guard<std::mutex> lk(mu_);
    samples_.push_back(latency_us);
  }
  std::vector<int64_t> get_sorted() {
    std::lock_guard<std::mutex> lk(mu_);
    std::sort(samples_.begin(), samples_.end());
    return samples_;
  }
  void clear() {
    std::lock_guard<std::mutex> lk(mu_);
    samples_.clear();
  }
 private:
  std::mutex mu_;
  std::vector<int64_t> samples_;
};

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------
struct Stats {
  int64_t count;
  double qps;
  double avg_ms;
  double p50_ms;
  double p95_ms;
  double p99_ms;
  double max_ms;
};

static double percentile(const std::vector<int64_t> &sorted, double pct) {
  if (sorted.empty()) return 0;
  int idx = (int)std::ceil(pct / 100.0 * sorted.size()) - 1;
  idx = std::max(0, std::min(idx, (int)sorted.size() - 1));
  return sorted[idx] / 1000.0;  // us -> ms
}

static Stats compute_stats(const std::vector<int64_t> &sorted, int duration_s) {
  Stats s{};
  s.count = sorted.size();
  if (s.count == 0) return s;
  int64_t total = 0;
  for (auto v : sorted) total += v;
  s.qps    = (double)s.count / duration_s;
  s.avg_ms = (double)total / s.count / 1000.0;
  s.p50_ms = percentile(sorted, 50);
  s.p95_ms = percentile(sorted, 95);
  s.p99_ms = percentile(sorted, 99);
  s.max_ms = sorted.back() / 1000.0;
  return s;
}

static const char *fmt_qps(double qps, char *buf) {
  if (qps >= 10000) snprintf(buf, 32, "%.1fK", qps / 1000);
  else snprintf(buf, 32, "%.0f", qps);
  return buf;
}

// ---------------------------------------------------------------------------
// MySQL helpers (for table setup and MySQL benchmark)
// ---------------------------------------------------------------------------
static MYSQL *mysql_connect_new() {
  MYSQL *m = mysql_init(nullptr);
  if (!m) die("mysql_init failed");
  if (!mysql_real_connect(m, g_mysql_host, g_mysql_user, g_mysql_pass,
                          g_database, g_mysql_port, nullptr, 0)) {
    fprintf(stderr, "MySQL connect error: %s\n", mysql_error(m));
    exit(1);
  }
  return m;
}

static void mysql_exec(MYSQL *m, const char *sql) {
  if (mysql_real_query(m, sql, strlen(sql))) {
    fprintf(stderr, "MySQL error: %s\nSQL: %s\n", mysql_error(m), sql);
    exit(1);
  }
  MYSQL_RES *res = mysql_store_result(m);
  if (res) mysql_free_result(res);
}

static int64_t mysql_count(MYSQL *m, const char *table) {
  char sql[256];
  snprintf(sql, sizeof(sql), "SELECT COUNT(*) FROM %s", table);
  mysql_real_query(m, sql, strlen(sql));
  MYSQL_RES *res = mysql_store_result(m);
  MYSQL_ROW row = mysql_fetch_row(res);
  int64_t cnt = atoll(row[0]);
  mysql_free_result(res);
  return cnt;
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------
static void create_and_load_table(MYSQL *m, const char *tbl, int target) {
  char sql[4096];
  snprintf(sql, sizeof(sql), "DROP TABLE IF EXISTS %s", tbl);
  mysql_exec(m, sql);

  snprintf(sql, sizeof(sql),
    "CREATE TABLE %s ("
    "  id INT NOT NULL AUTO_INCREMENT,"
    "  val1 BIGINT NOT NULL,"
    "  val2 BIGINT NOT NULL,"
    "  val3 DOUBLE NOT NULL,"
    "  val4 DOUBLE NOT NULL,"
    "  filter_date DATE NOT NULL,"
    "  padding VARCHAR(100) NOT NULL,"
    "  PRIMARY KEY (id),"
    "  INDEX idx_date (filter_date)"
    ") ENGINE=NDB", tbl);
  mysql_exec(m, sql);

  // Seed rows
  int seed = std::min(1000, target);
  std::string vals;
  for (int i = 0; i < seed; i++) {
    if (i) vals += ",";
    vals += "(FLOOR(RAND()*1000000), FLOOR(RAND()*1000000), "
            "RAND()*1000, RAND()*1000, "
            "DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND()*365) DAY), "
            "REPEAT(CHAR(65+FLOOR(RAND()*26)),80))";
  }
  std::string insert = "INSERT INTO " + std::string(tbl) +
    " (val1,val2,val3,val4,filter_date,padding) VALUES " + vals;
  mysql_exec(m, insert.c_str());

  int current = (int)mysql_count(m, tbl);
  while (current < target) {
    int remaining = target - current;
    int batch = std::min(remaining, g_batch_size);
    snprintf(sql, sizeof(sql),
      "INSERT INTO %s (val1,val2,val3,val4,filter_date,padding) "
      "SELECT FLOOR(RAND()*1000000), FLOOR(RAND()*1000000), "
      "RAND()*1000, RAND()*1000, "
      "DATE_ADD('2024-01-01', INTERVAL FLOOR(RAND()*365) DAY), "
      "REPEAT(CHAR(65+FLOOR(RAND()*26)),80) "
      "FROM %s LIMIT %d", tbl, tbl, batch);
    mysql_exec(m, sql);
    current = (int)mysql_count(m, tbl);
    fprintf(stderr, "  %s: %d / %d\n", tbl, current, target);
  }
}

static void prepare_tables(MYSQL *m) {
  for (size_t i = 0; i < g_tiers.size(); i++) {
    std::string tbl = table_name(g_tiers[i]);
    fprintf(stderr, "[%zu/%zu] Preparing %s (%d rows)...\n",
            i + 1, g_tiers.size(), tbl.c_str(), g_tiers[i]);
    create_and_load_table(m, tbl.c_str(), g_tiers[i]);
  }
  fprintf(stderr, "All tables ready.\n");
}

// ---------------------------------------------------------------------------
// NDB Pushdown Aggregation worker
// ---------------------------------------------------------------------------
// filter_date column id for Q4 filter — resolved at runtime
static int g_filter_date_col_id = -1;
// The encoded date value for '2024-07-01' as NDB DATE type
// NDB DATE is stored as 3-byte packed: (year*16+month)*32+day
static void encode_ndb_date(int year, int month, int day, char *buf) {
  Uint32 packed = (year * 16 + month) * 32 + day;
  buf[0] = (char)(packed & 0xFF);
  buf[1] = (char)((packed >> 8) & 0xFF);
  buf[2] = (char)((packed >> 16) & 0xFF);
}

static void pa_worker(Ndb_cluster_connection *conn,
                      const NdbDictionary::Table *table,
                      const NdbDictionary::Index *index,  // idx_date for Q4
                      int query_idx,
                      int duration_s,
                      LatencyCollector *collector,
                      std::atomic<bool> *ready,
                      std::atomic<bool> *start_flag) {
  Ndb myNdb(conn, g_database);
  if (myNdb.init()) die("Ndb::init failed in worker");

  // Pre-encode filter date for Q4
  char date_buf[3];
  encode_ndb_date(2024, 7, 1, date_buf);

  // Signal ready and wait for coordinated start
  ready->store(true);
  while (!start_flag->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  double deadline = now_sec() + duration_s;

  while (now_sec() < deadline) {
    int64_t t0 = now_ns();

    NdbTransaction *trans = myNdb.startTransaction();
    if (!trans) {
      NdbSleep_MilliSleep(10);
      continue;
    }

    NdbScanOperation *scanOp = nullptr;

    // Q4: use index scan on idx_date with bound (matches MySQL's index usage)
    if (query_idx == 3 && index != nullptr) {
      NdbIndexScanOperation *idxScanOp =
          trans->getNdbIndexScanOperation(index);
      if (!idxScanOp) {
        myNdb.closeTransaction(trans);
        continue;
      }
      idxScanOp->readTuples(NdbOperation::LM_CommittedRead);

      // filter_date >= '2024-07-01' → constant is lower bound → BoundLE
      if (idxScanOp->setBound("filter_date",
                               NdbIndexScanOperation::BoundLE,
                               date_buf) ||
          idxScanOp->end_of_bound(0)) {
        myNdb.closeTransaction(trans);
        continue;
      }
      scanOp = idxScanOp;  // NdbIndexScanOperation inherits NdbScanOperation
    } else {
      scanOp = trans->getNdbScanOperation(table);
      if (!scanOp) {
        myNdb.closeTransaction(trans);
        continue;
      }
      scanOp->readTuples(NdbOperation::LM_CommittedRead);
    }

    NdbAggregator aggregator(table);
    g_queries[query_idx].program(aggregator);
    aggregator.Finalize();

    if (scanOp->setAggregationCode(&aggregator) < 0 ||
        scanOp->DoAggregation() < 0) {
      myNdb.closeTransaction(trans);
      NdbSleep_MilliSleep(10);
      continue;
    }

    // Consume results (just iterate, don't print)
    aggregator.PrepareResults();
    NdbAggregator::ResultRecord record = aggregator.FetchResultRecord();
    while (!record.end()) {
      // Drain group-by columns
      NdbAggregator::Column col = record.FetchGroupbyColumn();
      while (!col.end()) col = record.FetchGroupbyColumn();
      // Drain aggregation results
      NdbAggregator::Result res = record.FetchAggregationResult();
      while (!res.end()) res = record.FetchAggregationResult();
      record = aggregator.FetchResultRecord();
    }

    myNdb.closeTransaction(trans);

    int64_t t1 = now_ns();
    collector->add((t1 - t0) / 1000);  // ns -> us
  }
}

// ---------------------------------------------------------------------------
// TierInfo — table + index pointers for each row tier
// ---------------------------------------------------------------------------
struct TierInfo {
  int rows;
  const NdbDictionary::Table *table;
  const NdbDictionary::Index *idx_date;  // for Q4 index scan
};

// ---------------------------------------------------------------------------
// Validation: run one PA query and compare results against MySQL
// ---------------------------------------------------------------------------
// Extract a double from a PA aggregation result regardless of its type
static double pa_result_as_double(NdbAggregator::Result &res) {
  switch (res.type()) {
    case NdbDictionary::Column::Bigint:
      return (double)res.data_int64();
    case NdbDictionary::Column::Bigunsigned:
      return (double)res.data_uint64();
    case NdbDictionary::Column::Double:
      return res.data_double();
    default:
      return (double)res.data_int64();
  }
}

static bool validate_query(Ndb_cluster_connection *conn,
                           MYSQL *mysql,
                           const NdbDictionary::Table *table,
                           const NdbDictionary::Index *index,
                           int tier, int query_idx) {
  const char *qname = g_queries[query_idx].name;
  std::string tbl = table_name(tier);

  // --- Run MySQL query ---
  char sql[512];
  snprintf(sql, sizeof(sql), g_queries[query_idx].sql_fmt, tbl.c_str());
  if (mysql_real_query(mysql, sql, strlen(sql))) {
    fprintf(stderr, "  FAIL %s @ %d rows: MySQL error: %s\n",
            qname, tier, mysql_error(mysql));
    return false;
  }
  MYSQL_RES *mres = mysql_store_result(mysql);
  if (!mres) {
    fprintf(stderr, "  FAIL %s @ %d rows: MySQL no result\n", qname, tier);
    return false;
  }
  MYSQL_ROW mrow = mysql_fetch_row(mres);
  if (!mrow) {
    fprintf(stderr, "  FAIL %s @ %d rows: MySQL empty result\n", qname, tier);
    mysql_free_result(mres);
    return false;
  }
  unsigned int num_fields = mysql_num_fields(mres);
  std::vector<double> mysql_vals;
  for (unsigned int i = 0; i < num_fields; i++) {
    mysql_vals.push_back(mrow[i] ? atof(mrow[i]) : 0.0);
  }
  mysql_free_result(mres);

  // --- Run PA query ---
  Ndb myNdb(conn, g_database);
  if (myNdb.init()) {
    fprintf(stderr, "  FAIL %s @ %d rows: Ndb::init failed\n", qname, tier);
    return false;
  }

  NdbTransaction *trans = myNdb.startTransaction();
  if (!trans) {
    fprintf(stderr, "  FAIL %s @ %d rows: startTransaction failed\n",
            qname, tier);
    return false;
  }

  NdbScanOperation *scanOp = nullptr;
  if (query_idx == 3 && index != nullptr) {
    NdbIndexScanOperation *idxScanOp =
        trans->getNdbIndexScanOperation(index);
    if (!idxScanOp) {
      fprintf(stderr, "  FAIL %s @ %d rows: getNdbIndexScanOperation failed\n",
              qname, tier);
      myNdb.closeTransaction(trans);
      return false;
    }
    idxScanOp->readTuples(NdbOperation::LM_CommittedRead);
    char date_buf[3];
    encode_ndb_date(2024, 7, 1, date_buf);
    idxScanOp->setBound("filter_date", NdbIndexScanOperation::BoundLE,
                         date_buf);
    idxScanOp->end_of_bound(0);
    scanOp = idxScanOp;
  } else {
    scanOp = trans->getNdbScanOperation(table);
    if (!scanOp) {
      fprintf(stderr, "  FAIL %s @ %d rows: getNdbScanOperation failed\n",
              qname, tier);
      myNdb.closeTransaction(trans);
      return false;
    }
    scanOp->readTuples(NdbOperation::LM_CommittedRead);
  }

  NdbAggregator aggregator(table);
  g_queries[query_idx].program(aggregator);
  aggregator.Finalize();

  if (scanOp->setAggregationCode(&aggregator) < 0 ||
      scanOp->DoAggregation() < 0) {
    fprintf(stderr, "  FAIL %s @ %d rows: DoAggregation failed: %s\n",
            qname, tier, trans->getNdbError().message);
    myNdb.closeTransaction(trans);
    return false;
  }

  // Collect PA results
  std::vector<double> pa_vals;
  aggregator.PrepareResults();
  NdbAggregator::ResultRecord record = aggregator.FetchResultRecord();
  while (!record.end()) {
    NdbAggregator::Column col = record.FetchGroupbyColumn();
    while (!col.end()) col = record.FetchGroupbyColumn();
    NdbAggregator::Result res = record.FetchAggregationResult();
    while (!res.end()) {
      pa_vals.push_back(pa_result_as_double(res));
      res = record.FetchAggregationResult();
    }
    record = aggregator.FetchResultRecord();
  }
  myNdb.closeTransaction(trans);

  // --- Compare ---
  if (pa_vals.size() != mysql_vals.size()) {
    fprintf(stderr, "  FAIL %s @ %d rows: column count mismatch "
            "(MySQL=%zu, PA=%zu)\n",
            qname, tier, mysql_vals.size(), pa_vals.size());
    return false;
  }

  bool pass = true;
  for (size_t i = 0; i < mysql_vals.size(); i++) {
    double m = mysql_vals[i];
    double p = pa_vals[i];
    double diff = std::abs(m - p);
    // Allow relative tolerance of 1e-6 or absolute tolerance of 1.0
    // (floating-point aggregation over many rows has rounding differences)
    double tol = std::max(1.0, std::abs(m) * 1e-6);
    if (diff > tol) {
      fprintf(stderr, "  FAIL %s @ %d rows: col[%zu] mismatch "
              "MySQL=%.6f PA=%.6f diff=%.6f\n",
              qname, tier, i, m, p, diff);
      pass = false;
    }
  }

  if (pass) {
    fprintf(stderr, "  PASS %s @ %d rows  (%zu values match)\n",
            qname, tier, pa_vals.size());
  }
  return pass;
}

static bool validate_all(Ndb_cluster_connection *conn,
                         MYSQL *mysql,
                         const std::vector<TierInfo> &tier_infos) {
  fprintf(stderr, "\n=== Validation: PA results vs MySQL ===\n");
  bool all_pass = true;
  for (int qi = 0; qi < NUM_QUERIES; qi++) {
    for (size_t ti = 0; ti < tier_infos.size(); ti++) {
      if (!validate_query(conn, mysql, tier_infos[ti].table,
                          tier_infos[ti].idx_date,
                          tier_infos[ti].rows, qi)) {
        all_pass = false;
      }
    }
  }
  if (all_pass) {
    fprintf(stderr, "=== All validations PASSED ===\n\n");
  } else {
    fprintf(stderr, "=== Some validations FAILED ===\n\n");
  }
  return all_pass;
}

// ---------------------------------------------------------------------------
// MySQL worker
// ---------------------------------------------------------------------------
static void mysql_worker(const char *sql,
                         int duration_s,
                         LatencyCollector *collector,
                         std::atomic<bool> *ready,
                         std::atomic<bool> *start_flag) {
  MYSQL *m = mysql_connect_new();

  ready->store(true);
  while (!start_flag->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  double deadline = now_sec() + duration_s;

  while (now_sec() < deadline) {
    int64_t t0 = now_ns();

    if (mysql_real_query(m, sql, strlen(sql)) == 0) {
      MYSQL_RES *res = mysql_store_result(m);
      if (res) {
        // Drain all rows
        while (mysql_fetch_row(res)) {}
        mysql_free_result(res);
      }
    }

    int64_t t1 = now_ns();
    collector->add((t1 - t0) / 1000);
  }

  mysql_close(m);
}

// ---------------------------------------------------------------------------
// Run workers for one (engine, tier, query) combination
// ---------------------------------------------------------------------------
static Stats run_workers(const char *engine,
                         std::vector<Ndb_cluster_connection*> &ndb_conns,
                         const NdbDictionary::Table *table,
                         const NdbDictionary::Index *index,
                         int tier, int query_idx,
                         int duration_s) {
  LatencyCollector collector;
  collector.reserve(g_concurrency * duration_s * 5000);  // rough estimate

  std::vector<std::thread> threads;
  std::vector<std::atomic<bool>> ready(g_concurrency);
  std::atomic<bool> start_flag{false};

  for (int i = 0; i < g_concurrency; i++) {
    ready[i].store(false);
  }

  // Build MySQL query string (for mysql workers)
  char sql[512];
  snprintf(sql, sizeof(sql), g_queries[query_idx].sql_fmt,
           table_name(tier).c_str());

  for (int i = 0; i < g_concurrency; i++) {
    if (strcmp(engine, "ndb_pa") == 0) {
      // Round-robin distribute workers across NDB connections
      Ndb_cluster_connection *conn = ndb_conns[i % ndb_conns.size()];
      threads.emplace_back(pa_worker,
          conn, table, index, query_idx, duration_s,
          &collector, &ready[i], &start_flag);
    } else {
      threads.emplace_back(mysql_worker,
          sql, duration_s,
          &collector, &ready[i], &start_flag);
    }
  }

  // Wait for all workers to be ready
  for (int i = 0; i < g_concurrency; i++) {
    while (!ready[i].load()) std::this_thread::yield();
  }

  // Coordinated start
  start_flag.store(true, std::memory_order_release);

  for (auto &t : threads) t.join();

  auto sorted = collector.get_sorted();
  return compute_stats(sorted, duration_s);
}

// ---------------------------------------------------------------------------
// Result printing
// ---------------------------------------------------------------------------
static void print_query_header(const char *qname) {
  printf("\n  Query: %s\n", qname);
  printf("  %.*s\n", 90, "-----------------------------------------------------------------------------------------");
  printf("  %8s  |  %-40s  |  %-40s  | %s\n", "", "--- MySQL ---", "--- NDB PA (direct) ---", "");
  printf("  %8s  |  %8s %7s %7s %7s %7s  |  %8s %7s %7s %7s %7s  | %6s\n",
         "Rows", "QPS", "Avg", "P50", "P95", "P99",
         "QPS", "Avg", "P50", "P95", "P99", "Ratio");
  printf("  %8s  |  %8s %7s %7s %7s %7s  |  %8s %7s %7s %7s %7s  | %6s\n",
         "", "(q/s)", "(ms)", "(ms)", "(ms)", "(ms)",
         "(q/s)", "(ms)", "(ms)", "(ms)", "(ms)", "");
  printf("  %.*s\n", 90, "-----------------------------------------------------------------------------------------");
}

static void print_row(int rows, const Stats &ms, const Stats &ps) {
  char mbuf[32], pbuf[32];
  const char *ratio_str = "N/A";
  char ratio_buf[32];
  if (ms.qps > 0) {
    snprintf(ratio_buf, sizeof(ratio_buf), "%.1fx", ps.qps / ms.qps);
    ratio_str = ratio_buf;
  }
  printf("  %8d  |  %8s %7.1f %7.1f %7.1f %7.1f  |  %8s %7.1f %7.1f %7.1f %7.1f  | %6s\n",
         rows,
         fmt_qps(ms.qps, mbuf), ms.avg_ms, ms.p50_ms, ms.p95_ms, ms.p99_ms,
         fmt_qps(ps.qps, pbuf), ps.avg_ms, ps.p50_ms, ps.p95_ms, ps.p99_ms,
         ratio_str);
}

// ---------------------------------------------------------------------------
// Parse arguments
// ---------------------------------------------------------------------------
static void parse_args(int argc, char **argv) {
  for (int i = 1; i < argc; i++) {
    std::string arg = argv[i];
    if ((arg == "-c" || arg == "--connect-string") && i + 1 < argc) {
      g_ndb_connectstring = argv[++i];
    } else if ((arg == "-h" || arg == "--mysql-host") && i + 1 < argc) {
      g_mysql_host = argv[++i];
    } else if ((arg == "-P" || arg == "--mysql-port") && i + 1 < argc) {
      g_mysql_port = atoi(argv[++i]);
    } else if ((arg == "-u" || arg == "--mysql-user") && i + 1 < argc) {
      g_mysql_user = argv[++i];
    } else if ((arg == "-p" || arg == "--mysql-password") && i + 1 < argc) {
      g_mysql_pass = argv[++i];
    } else if ((arg == "-d" || arg == "--database") && i + 1 < argc) {
      g_database = argv[++i];
    } else if (arg == "--ndb-connections" && i + 1 < argc) {
      g_ndb_connections = atoi(argv[++i]);
      if (g_ndb_connections < 1) g_ndb_connections = 1;
    } else if (arg == "--concurrency" && i + 1 < argc) {
      g_concurrency = atoi(argv[++i]);
    } else if (arg == "--duration" && i + 1 < argc) {
      g_duration = atoi(argv[++i]);
    } else if (arg == "--warmup" && i + 1 < argc) {
      g_warmup = atoi(argv[++i]);
    } else if (arg == "--tiers" && i + 1 < argc) {
      g_tiers.clear();
      char *tok = strtok(argv[++i], ",");
      while (tok) {
        g_tiers.push_back(atoi(tok));
        tok = strtok(nullptr, ",");
      }
    } else if (arg == "--validate") {
      g_validate = true;
    } else if (arg == "--quick") {
      g_tiers = {100, 10000, 500000};
      g_duration = 15;
      g_warmup = 3;
      g_concurrency = 16;
    } else if (arg == "--help") {
      printf("Usage: %s -c <ndb_connectstring> -h <mysql_host> [options]\n"
             "  -c, --connect-string   NDB connect string (default: localhost:1186)\n"
             "  -h, --mysql-host       MySQL host (default: 127.0.0.1)\n"
             "  -P, --mysql-port       MySQL port (default: 3306)\n"
             "  -u, --mysql-user       MySQL user (default: root)\n"
             "  -p, --mysql-password   MySQL password\n"
             "  -d, --database         Database name (default: bench_pa_qps)\n"
             "  --ndb-connections N    NDB cluster connections for PA (default: 4)\n"
             "  --concurrency N        Connections per engine (default: 32)\n"
             "  --duration N           Seconds per data point (default: 60)\n"
             "  --warmup N             Warmup seconds (default: 5)\n"
             "  --tiers 100,500,...    Row tiers (default: 100,500,10000,100000,500000)\n"
             "  --validate             Validate PA results against MySQL before benchmarking\n"
             "  --quick                Quick mode (fewer tiers, 15s, 16 conn)\n",
             argv[0]);
      exit(0);
    } else {
      fprintf(stderr, "Unknown option: %s (try --help)\n", arg.c_str());
      exit(1);
    }
  }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------
int main(int argc, char **argv) {
  parse_args(argc, argv);

  // --- NDB init: create multiple cluster connections (like mysqld) ---
  ndb_init();
  std::vector<Ndb_cluster_connection*> ndb_conns;
  for (int i = 0; i < g_ndb_connections; i++) {
    auto *conn = new Ndb_cluster_connection(g_ndb_connectstring);
    char name[64];
    snprintf(name, sizeof(name), "bench_pa_qps_%d", i);
    conn->set_name(name);
    if (conn->connect(5, 3, 1) != 0) die("Cannot connect to NDB mgmd");
    if (conn->wait_until_ready(30, 0) != 0) die("NDB cluster not ready");
    ndb_conns.push_back(conn);
  }
  fprintf(stderr, "Created %d NDB cluster connections.\n", g_ndb_connections);

  // --- MySQL init ---
  MYSQL *setup_mysql = mysql_connect_new();

  // Get cluster info
  {
    char sql[256];
    snprintf(sql, sizeof(sql), "CREATE DATABASE IF NOT EXISTS %s", g_database);
    mysql_real_query(setup_mysql, sql, strlen(sql));
    MYSQL_RES *res = mysql_store_result(setup_mysql);
    if (res) mysql_free_result(res);
  }
  // Switch to database
  mysql_select_db(setup_mysql, g_database);

  // Print header
  printf("\n");
  printf("=========================================================================\n");
  printf("  NDB Pushdown Aggregation (direct API) vs MySQL — QPS Benchmark\n");
  printf("=========================================================================\n");
  printf("\n");
  printf("  NDB:          %s (%d cluster connections)\n",
         g_ndb_connectstring, g_ndb_connections);
  printf("  MySQL:        %s:%d\n", g_mysql_host, g_mysql_port);
  printf("  Database:     %s\n", g_database);
  printf("  Concurrency:  %d workers per engine\n", g_concurrency);
  printf("  Duration:     %ds measured + %ds warmup\n", g_duration, g_warmup);
  printf("  Row tiers:    ");
  for (size_t i = 0; i < g_tiers.size(); i++) {
    if (i) printf(", ");
    printf("%d", g_tiers[i]);
  }
  printf("\n\n");

  auto bench_start = std::chrono::steady_clock::now();

  // Phase 1: Prepare tables
  prepare_tables(setup_mysql);

  // Phase 2: Get NDB table objects, indexes, and resolve column IDs
  std::vector<TierInfo> tier_infos;
  {
    Ndb myNdb(ndb_conns[0], g_database);
    if (myNdb.init()) die("Ndb::init failed");

    for (int rows : g_tiers) {
      std::string tbl = table_name(rows);
      const NdbDictionary::Dictionary *dict = myNdb.getDictionary();
      const NdbDictionary::Table *table = dict->getTable(tbl.c_str());
      if (!table) {
        fprintf(stderr, "FATAL: Cannot find table %s: %s\n",
                tbl.c_str(), dict->getNdbError().message);
        exit(1);
      }

      // Resolve idx_date ordered index for Q4 index scan
      const NdbDictionary::Index *idx_date =
          dict->getIndex("idx_date", tbl.c_str());
      if (!idx_date) {
        fprintf(stderr, "WARNING: Cannot find index idx_date on %s: %s\n",
                tbl.c_str(), dict->getNdbError().message);
      }

      tier_infos.push_back({rows, table, idx_date});

      // Resolve filter_date column ID from first table (for fallback)
      if (g_filter_date_col_id < 0) {
        const NdbDictionary::Column *col = table->getColumn("filter_date");
        if (col) g_filter_date_col_id = col->getColumnNo();
      }
    }
  }  // myNdb destroyed here, before connections are deleted

  fprintf(stderr, "Tables resolved. filter_date col_id=%d, idx_date=%s\n",
          g_filter_date_col_id,
          tier_infos[0].idx_date ? "found" : "NOT found");

  // Phase 2.5: Validate PA results against MySQL (if requested)
  if (g_validate) {
    if (!validate_all(ndb_conns[0], setup_mysql, tier_infos)) {
      fprintf(stderr, "Validation failed. Aborting benchmark.\n");
      mysql_close(setup_mysql);
      for (auto *conn : ndb_conns) delete conn;
      ndb_end(0);
      return 1;
    }
  }

  // Phase 3: Run benchmarks
  // Store summary data: summary[tier_idx] = {sum_m_qps, sum_m_avg, ... }
  struct SummaryEntry {
    double m_qps_sum = 0, m_avg_sum = 0, m_p95_sum = 0;
    double p_qps_sum = 0, p_avg_sum = 0, p_p95_sum = 0;
    int n = 0;
  };
  std::vector<SummaryEntry> summary(g_tiers.size());

  // CSV output
  std::string csv_path = std::string(getenv("HOME") ? getenv("HOME") : "/tmp") +
      "/bench_results/bench_pa_qps_" +
      std::to_string(time(nullptr)) + ".csv";
  {
    // Ensure directory exists
    std::string dir = csv_path.substr(0, csv_path.rfind('/'));
    std::string cmd = "mkdir -p " + dir;
    system(cmd.c_str());
  }
  FILE *csv = fopen(csv_path.c_str(), "w");
  if (csv) {
    fprintf(csv, "rows,query,engine,concurrency,duration_s,"
                 "total_queries,qps,avg_ms,p50_ms,p95_ms,p99_ms,max_ms\n");
  }

  for (int qi = 0; qi < NUM_QUERIES; qi++) {
    print_query_header(g_queries[qi].name);

    for (size_t ti = 0; ti < tier_infos.size(); ti++) {
      int rows = tier_infos[ti].rows;
      const NdbDictionary::Table *table = tier_infos[ti].table;
      const NdbDictionary::Index *index = tier_infos[ti].idx_date;

      // Warmup
      fprintf(stderr, "Warmup: %d rows, %s, %d conn, %ds...\n",
              rows, g_queries[qi].name, g_concurrency, g_warmup);
      run_workers("mysql", ndb_conns, table, index, rows, qi, g_warmup);
      run_workers("ndb_pa", ndb_conns, table, index, rows, qi, g_warmup);

      // MySQL measured
      fprintf(stderr, "MySQL:  %d rows, %s, %d conn, %ds...\n",
              rows, g_queries[qi].name, g_concurrency, g_duration);
      Stats ms = run_workers("mysql", ndb_conns, table, index, rows, qi, g_duration);

      // NDB PA measured
      fprintf(stderr, "NDB PA: %d rows, %s, %d conn, %ds...\n",
              rows, g_queries[qi].name, g_concurrency, g_duration);
      Stats ps = run_workers("ndb_pa", ndb_conns, table, index, rows, qi, g_duration);

      print_row(rows, ms, ps);

      // CSV
      if (csv) {
        fprintf(csv, "%d,%s,mysql,%d,%d,%ld,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f\n",
                rows, g_queries[qi].name, g_concurrency, g_duration,
                ms.count, ms.qps, ms.avg_ms, ms.p50_ms, ms.p95_ms,
                ms.p99_ms, ms.max_ms);
        fprintf(csv, "%d,%s,ndb_pa,%d,%d,%ld,%.1f,%.1f,%.1f,%.1f,%.1f,%.1f\n",
                rows, g_queries[qi].name, g_concurrency, g_duration,
                ps.count, ps.qps, ps.avg_ms, ps.p50_ms, ps.p95_ms,
                ps.p99_ms, ps.max_ms);
        fflush(csv);
      }

      // Accumulate summary
      summary[ti].m_qps_sum += ms.qps;
      summary[ti].m_avg_sum += ms.avg_ms;
      summary[ti].m_p95_sum += ms.p95_ms;
      summary[ti].p_qps_sum += ps.qps;
      summary[ti].p_avg_sum += ps.avg_ms;
      summary[ti].p_p95_sum += ps.p95_ms;
      summary[ti].n++;
    }
    printf("  %.*s\n", 90, "-----------------------------------------------------------------------------------------");
  }

  // Phase 4: Summary
  printf("\n");
  printf("=========================================================================\n");
  printf("  SUMMARY — Average across all queries\n");
  printf("=========================================================================\n");
  printf("\n");
  printf("  %8s  |  %10s %8s %8s  |  %10s %8s %8s  |  %6s\n",
         "Rows", "MySQL QPS", "Avg(ms)", "P95(ms)",
         "NDB PA QPS", "Avg(ms)", "P95(ms)", "Ratio");
  printf("  %.*s\n", 78, "-----------------------------------------------------------------------------------------");

  for (size_t ti = 0; ti < tier_infos.size(); ti++) {
    if (summary[ti].n == 0) continue;
    double n = summary[ti].n;
    double m_qps = summary[ti].m_qps_sum / n;
    double m_avg = summary[ti].m_avg_sum / n;
    double m_p95 = summary[ti].m_p95_sum / n;
    double p_qps = summary[ti].p_qps_sum / n;
    double p_avg = summary[ti].p_avg_sum / n;
    double p_p95 = summary[ti].p_p95_sum / n;

    char mbuf[32], pbuf[32], rbuf[32];
    snprintf(rbuf, sizeof(rbuf), "%.1fx", m_qps > 0 ? p_qps / m_qps : 0);

    printf("  %8d  |  %10s %8.1f %8.1f  |  %10s %8.1f %8.1f  |  %6s\n",
           tier_infos[ti].rows,
           fmt_qps(m_qps, mbuf), m_avg, m_p95,
           fmt_qps(p_qps, pbuf), p_avg, p_p95,
           rbuf);
  }
  printf("  %.*s\n", 78, "-----------------------------------------------------------------------------------------");

  if (csv) {
    fclose(csv);
    printf("\n  CSV: %s\n", csv_path.c_str());
  }

  auto bench_end = std::chrono::steady_clock::now();
  int elapsed = (int)std::chrono::duration_cast<std::chrono::seconds>(
      bench_end - bench_start).count();
  printf("\n  Completed in %dm %ds\n\n", elapsed / 60, elapsed % 60);

  mysql_close(setup_mysql);
  for (auto *conn : ndb_conns) delete conn;
  ndb_conns.clear();
  ndb_end(0);
  return 0;
}
