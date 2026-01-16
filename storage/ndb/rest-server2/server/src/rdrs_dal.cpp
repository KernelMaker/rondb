/*
 * Copyright (C) 2023, 2025 Hopsworks AB
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

#include "rdrs_dal.h"
#include "db_operations/pk/pkr_operation.hpp"
#include "db_operations/ronsql/ronsql_operation.hpp"
#include "rdrs_dal.hpp"
#include "rdrs_rondb_connection_pool.hpp"
#include "retry_handler.hpp"
#include "status.hpp"
#include "logger.hpp"
#include "pk_data_structs.hpp"

#include <storage/ndb/include/ndb_global.h>
#include <util/require.h>
#include <mgmapi.h>
#include <my_base.h>
#include <unistd.h>
#include <NdbApi.hpp>
#include <cstdlib>
#include <cstring>
#include <EventLogger.hpp>

#include <rapidjson/fwd.h>
#include <rapidjson/document.h>      // rapidjson::Document
#include <rapidjson/prettywriter.h>  // rapidjson::PrettyWriter
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>  // rapidjson::Writer
#include "my_byteorder.h"

#include <my_time.h>

extern EventLogger *g_eventLogger;

#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"
#include "string_with_len.h"

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_DAL 1
#endif

#ifdef DEBUG_DAL
#define DEB_TRACE() do { \
  printf("rdrs_dal.cpp:%d\n", __LINE__); \
  fflush(stdout); \
} while (0)
#else
#define DEB_TRACE() do { } while (0)
#endif


RDRSRonDBConnectionPool *rdrsRonDBConnectionPool = nullptr;

RS_Status init(unsigned int numThreads, unsigned int num_data_connections) {
  // disable buffered stdout
  setbuf(stdout, NULL);

  // Initialize NDB Connection and Object Pool
  rdrsRonDBConnectionPool = new RDRSRonDBConnectionPool();
  RS_Status status = rdrsRonDBConnectionPool->Init(numThreads,
                                                   num_data_connections);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status add_data_connection(const char *connection_string,
                              unsigned int connection_pool_size,
                              unsigned int *node_ids,
                              unsigned int node_ids_len,
                              unsigned int connection_retries,
                              unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddConnections(
    connection_string,
    connection_pool_size,
    node_ids,
    node_ids_len,
    connection_retries,
    connection_retry_delay_in_sec);

  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status add_metadata_connection(const char *connection_string,
                                  unsigned int connection_pool_size,
                                  unsigned int *node_ids,
                                  unsigned int node_ids_len,
                                  unsigned int connection_retries,
                                  unsigned int connection_retry_delay_in_sec) {

  RS_Status status = rdrsRonDBConnectionPool->AddMetaConnections(
    connection_string,
    connection_pool_size,
    node_ids,
    node_ids_len,
    connection_retries,
    connection_retry_delay_in_sec);

  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  return RS_OK;
}

RS_Status set_data_cluster_op_retry_props(
  const unsigned int retry_cont,
  const unsigned int rety_initial_delay,
  const unsigned int jitter) {
  DATA_CONN_OP_RETRY_COUNT = retry_cont;
  DATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  DATA_CONN_OP_RETRY_JITTER_IN_MS = jitter;
  return RS_OK;
}

RS_Status set_metadata_cluster_op_retry_props(
  const unsigned int retry_cont,
  const unsigned int rety_initial_delay,
  const unsigned int jitter) {
  METADATA_CONN_OP_RETRY_COUNT = retry_cont;
  METADATA_CONN_OP_RETRY_INITIAL_DELAY_IN_MS = rety_initial_delay;
  METADATA_CONN_OP_RETRY_JITTER_IN_MS = jitter;
  return RS_OK;
}

RS_Status shutdown_connection() {
  rdrsRonDBConnectionPool->shutdown();
  delete rdrsRonDBConnectionPool;
  return RS_OK;
}

RS_Status reconnect() {
  return rdrsRonDBConnectionPool->Reconnect();
}

RS_Status pk_batch_read(void *amalloc_void,
                        unsigned int no_req,
                        bool is_batch,
                        RS_Buffer *req_buffs,
                        RS_Buffer *resp_buffs,
                        unsigned int threadIndex) {
  ArenaMalloc *amalloc = (ArenaMalloc*)amalloc_void;
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  DATA_OP_RETRY_HANDLER(
    BatchKeyOperations pkread;
    status = pkread.perform_operation(amalloc,
                                      no_req,
                                      is_batch,
                                      req_buffs,
                                      resp_buffs,
                                      ndb_object);
  )
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  return status;
}

RS_Status ronsql_dal(const char* database,
                     RonSQLExecParams* ep,
                     unsigned int threadIndex) {
  assert(ep != nullptr);
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    DEB_TRACE();
    return status;
  }

  assert(ep->ndb == NULL);
  assert(ndb_object != NULL);
  ep->ndb = ndb_object;
  const char* saved_database_name = ndb_object->getDatabaseName();
  ndb_object->setDatabaseName(database);
  DEB_TRACE();
  status = ronsql_op(*ep);
  DEB_TRACE();
  ndb_object->setDatabaseName(saved_database_name);
  ep->ndb = NULL;
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  DEB_TRACE();
  return status;
}

/**
 * Returns statistis about RonDB connection
 */
RS_Status get_rondb_stats(RonDB_Stats *stats) {
  RonDB_Stats ret = rdrsRonDBConnectionPool->GetStats();
  stats->ndb_objects_created = ret.ndb_objects_created;
  stats->ndb_objects_deleted = ret.ndb_objects_deleted;
  stats->ndb_objects_count = ret.ndb_objects_count;
  stats->ndb_objects_available = ret.ndb_objects_available;
  stats->connection_state = ret.connection_state;
  return RS_OK;
}

void*
get_rdrs_ndb_object(int thread_index) {
  Ndb *ndb_object  = nullptr;
  (void)rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                              thread_index);
  return (void*)ndb_object;
}

void
return_rdrs_ndb_object(void *ndb_object, int thread_index) {
  RS_Status status = RS_OK;
  rdrsRonDBConnectionPool->ReturnNdbObject((Ndb*)ndb_object,
                                           &status,
                                           thread_index);
}
CRS_Status CRS_Status::SUCCESS = CRS_Status(HTTP_CODE::SUCCESS);

class Bitmap {
 public:
   Bitmap(int n_cols)
     : n_bytes_((n_cols + 7) / 8), bitmap_(nullptr) {
   }

   ~Bitmap() {
     delete[] bitmap_;
   }

   Bitmap(const Bitmap&) = delete;
   Bitmap& operator=(const Bitmap&) = delete;

   bool Init() {
     if (!bitmap_) {
       bitmap_ = new(std::nothrow) unsigned char[n_bytes_];
       if (!bitmap_) {
         return false;
       }
       memset(bitmap_, 0, n_bytes_);
     }
     return true;
   }

   void SetBit(int col) {
     if (col < 0 || col >= n_bytes_ * 8) {
       return;
     }
     int idx = col / 8;
     int offset = col & 7;
     bitmap_[idx] |= (static_cast<unsigned char>(1) << offset);
     // std::cout << "bitmap: "
     //   << static_cast<int>(bitmap_[idx]) << std::endl;
   }

   bool GetBit(int col) const {
     if (col < 0 || col >= n_bytes_ * 8) {
       return false;
     }
     int idx = col / 8;
     int offset = col & 7;
     return (bitmap_[idx] & (static_cast<unsigned char>(1) << offset)) != 0;
   }

   const unsigned char* bitmap() const {
     return bitmap_;
   }

 private:
   int n_bytes_;
   unsigned char* bitmap_;
};

RS_Status GenerateBinary(Node& node, std::vector<uint8_t>& bin) {
  RS_Status status = RS_OK;
  assert(node.col != nullptr);
  bin.clear();
  switch(node.col->getType()) {
    case NdbDictionary::Column::Tinyint: {
      int8_t x = node.value.i64;
      bin.push_back(*reinterpret_cast<int8_t*>(&x));
      break;
    }
    case NdbDictionary::Column::Smallint: {
      int16_t x = node.value.i64;
      bin.resize(sizeof(int16_t));
      int2store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Mediumint: {
      int32_t x = node.value.i64;
      bin.resize(3);
      int3store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Int: {
      int32_t x = node.value.i64;
      bin.resize(sizeof(int32_t));
      int4store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Bigint: {
      bin.resize(sizeof(int64_t));
      int8store(bin.data(), node.value.i64);
      break;
    }
    case NdbDictionary::Column::Tinyunsigned: {
      uint8_t x = node.value.u64;
      bin.push_back(*reinterpret_cast<uint8_t*>(&x));
      break;
    }
    case NdbDictionary::Column::Smallunsigned: {
      uint16_t x = node.value.u64;
      bin.resize(sizeof(uint16_t));
      int2store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Mediumunsigned: {
      uint32_t x = node.value.u64;
      bin.resize(3);
      int3store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Unsigned: {
      uint32_t x = node.value.u64;
      bin.resize(sizeof(uint32_t));
      int4store(bin.data(), x);
      break;
    }
    case NdbDictionary::Column::Bigunsigned: {
      bin.resize(sizeof(int64_t));
      int8store(bin.data(), node.value.u64);
      break;
    }
    case NdbDictionary::Column::Varchar: {
      if (node.value.s.size() > node.col->getLength()) {
        status = RS_CLIENT_ERROR("The provided string is too long. Column: " +
            std::string(node.col->getName()));
        break;
      }
      assert(node.value.s.size() <= (uint16_t)(0xFF));
      uint8_t len = node.value.s.size();
      bin.resize(len + 1);
      bin[0] = len;
      memcpy(&bin[1], node.value.s.data(), len);
      break;
    }
    case NdbDictionary::Column::Longvarchar: {
      if (node.value.s.size() > node.col->getLength()) {
        status = RS_CLIENT_ERROR("The provided string is too long. Column: " +
            std::string(node.col->getName()));
        break;
      }
      assert(node.value.s.size() <= (uint16_t)(0xFFFF));
      uint16_t len = node.value.s.size();
      bin.resize(len + 2);
      int2store(bin.data(), len);
      memcpy(&bin[2], node.value.s.data(), len);
      break;
    }
    case NdbDictionary::Column::Timestamp2: {
      if (node.value.kind == Node::ParsedValue::Kind::STRING) {
        uint32_t precision = node.col->getPrecision();
        MYSQL_TIME lTime;
        MYSQL_TIME_STATUS time_status;
        bool ret = str_to_datetime(node.value.s.data(), node.value.s.length(), &lTime, 0, &time_status);
        if (unlikely(ret != 0)) {
          status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
          break;
        }
        time_t epoch = 0;
        struct tm time_info;
        time_info.tm_year = lTime.year - 1900;  // tm_year is years since 1900
        time_info.tm_mon = lTime.month - 1;     // tm_mon is 0-based
        time_info.tm_mday = lTime.day;
        time_info.tm_hour = lTime.hour;
        time_info.tm_min = lTime.minute;
        time_info.tm_sec = lTime.second;
        time_info.tm_isdst = -1; // Daylight saving t
        epoch = timegm(&time_info);
        // 1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
        if (unlikely(epoch <= 0 || epoch > 2147483647)) {
          status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) + std::string(" Column: ") +
            std::string(node.col->getName()));
          break;
        }
        int warnings = 0;
        my_datetime_adjust_frac(&lTime, precision, &warnings, true);
        if (unlikely(warnings != 0)) {
          status = RS_CLIENT_ERROR(
            std::string(rdrsErrorMessage(ERROR_INVALID_DATE_TIME)) +
            std::string(" Column: ") + std::string(node.col->getName()));
          break;
        }
        // On Mac timeval.tv_usec is Int32 and on linux it is Int64.
        // Inorder to be compatible we cast l_time.second_part to Int32
        // This will not create problems as only six digit nanoseconds
        // are stored in Timestamp2
        my_timeval myTV{epoch, (Int32)lTime.second_part};
        bin.resize(7);
        my_timestamp_to_binary(&myTV, (uchar *)bin.data(), precision);
        break;
      } else {
      }
      break;
    }
    default: {
      assert(0);
      break;
    }
  }
  return status;
}

RS_Status BindFilterColumns(std::shared_ptr<FilterNode>& node,
                            const NdbDictionary::Table* table) {
  RS_Status status = RS_OK;
  if (node == nullptr) {
    return status;
  }
  if (node->type != FilterNode::Type::LOGIC) {
    const NdbDictionary::Column *column = table->getColumn(node->column.c_str());
    if (column == nullptr) {
      status = RS_CLIENT_404_WITH_MSG_ERROR(
        "The column used in filter doesn't exist in table");
      return status;
    }
    assert(node->col == nullptr);
    node->col = column;
  } else {
    for (auto& child : node->children) {
      status = BindFilterColumns(child, table);
      if (status.http_code != HTTP_CODE::SUCCESS) {
        break;
      }
    }
  }
  return status;
}

RS_Status CompileFilter(std::shared_ptr<FilterNode>& node,
                        NdbScanFilter* filter) {
  RS_Status status = RS_OK;
  if (node == nullptr) {
    return status;
  }
  if (node->type == FilterNode::Type::LOGIC) {
    DEB_SCAN("  filter->begin(" << node->group << ")" << std::endl);
    filter->begin(node->group);
  } else {
    assert(node->col != nullptr);
    switch (node->type) {
      case FilterNode::Type::COMPARE:
        status = GenerateBinary(*node, node->binary);
        if (status.http_code != HTTP_CODE::SUCCESS) {
          return status;
        }
        DEB_SCAN_BLOCK(
          std::cout << "  filter->cmp(" << node->cond << ", "
                    << node->col->getAttrId() << ", ";
          std::cout << "[" << node->binary.size() << "]";
          for (auto byte : node->binary) {
            std::cout << std::hex << std::setw(2) << std::setfill('0')
                      << static_cast<int>(byte) << ' ';
          }
          std::cout << std::dec << ")" << std::endl;
        );
        filter->cmp(node->cond, node->col->getAttrId(), node->binary.data(), node->binary.size());
        break;
      case FilterNode::Type::IS_NULL:
        DEB_SCAN("  filter->isnull(" << node->col->getAttrId() << ")" << std::endl);
        filter->isnull(node->col->getAttrId());
        break;
      case FilterNode::Type::IS_NOT_NULL:
        DEB_SCAN("  filter->isnotnull(" << node->col->getAttrId() << ")" << std::endl);
        filter->isnotnull(node->col->getAttrId());
        break;
      default:
        status = RS_CLIENT_404_WITH_MSG_ERROR(
            "Invalid filter node type");
        return status;
    }
  }

  for (auto& child : node->children) {
    status = CompileFilter(child, filter);
    if (status.http_code != HTTP_CODE::SUCCESS) {
      break;
    }
  }
  if (node->type == FilterNode::Type::LOGIC) {
    DEB_SCAN("  filter->end()" << std::endl);
    filter->end();
  }
  return status;
}

RS_Status BindIndexColumns(IndexScanParams& index_params,
                            const NdbDictionary::Table* table,
                            const NdbDictionary::Index* index) {
  RS_Status status = RS_OK;
  
  assert(table != nullptr);
  assert(index != nullptr);
  assert(!index_params.columns.empty());
  if (index_params.columns.size() != index->getNoOfColumns()) {
      status = RS_CLIENT_404_WITH_MSG_ERROR(
        "key_columns don't match the index columns");
      return status;
  }
  for (int i = 0; i < index->getNoOfColumns(); i++) {
    const NdbDictionary::Column* column = index->getColumn(i);
    if (std::string(column->getName()) != index_params.columns[i]) {
        status = RS_CLIENT_404_WITH_MSG_ERROR(
          "key_columns don't match the index columns");
        return status;
    }
  }

  assert(index_params.cols.empty());
  for (auto& column_name : index_params.columns) {
    const NdbDictionary::Column *column = table->getColumn(column_name.c_str());
    if (column == nullptr) {
      status = RS_CLIENT_404_WITH_MSG_ERROR(
        "The column used in filter doesn't exist in table");
      return status;
    }
    index_params.cols.push_back(column);
  }
  return status;
}


typedef rapidjson::UTF8<char> RJ_Encoding;
typedef rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator> RJ_Allocator;
typedef rapidjson::GenericDocument<RJ_Encoding, RJ_Allocator,
                                   rapidjson::CrtAllocator>
    RJ_Document;
typedef rapidjson::GenericValue<RJ_Encoding, RJ_Allocator> RJ_Value;
typedef rapidjson::GenericStringBuffer<RJ_Encoding, rapidjson::CrtAllocator>
    RJ_StringBuffer;
typedef rapidjson::PrettyWriter<RJ_StringBuffer, RJ_Encoding, RJ_Encoding,
                                RJ_Allocator, 0>
    RJ_PrettyWriter;

using RJ_Writer = rapidjson::Writer<RJ_StringBuffer, RJ_Encoding, RJ_Encoding,
                                    RJ_Allocator, 0>;
void WriteColumnData2Json(RJ_Writer& writer, Uint32 attrType, const NdbDictionary::Column* col,
                          const char* binary) {
  if (binary == nullptr) {
    return;
  }
  Uint16 varchar_len = 0;
  Int64 value_int64 = 0;
  Uint64 value_uint64 = 0;
  float value_float = 0.0;
  double value_double = 0.0;
  const char* field = binary;
  switch(attrType) {
    case NdbDictionary::Column::Tinyint:
      value_int64 = *reinterpret_cast<const int8_t*>(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Tinyunsigned:
      value_uint64 = *reinterpret_cast<const uint8_t*>(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Smallint:
      value_int64 = sint2korr(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Smallunsigned:
      value_uint64 = uint2korr(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Mediumint:
      value_int64 = sint3korr(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Mediumunsigned:
      value_uint64 = uint3korr(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Int:
      value_int64 = *reinterpret_cast<const int32_t*>(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Unsigned:
      value_uint64 = *reinterpret_cast<const uint32_t*>(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Bigint:
      value_int64 = *reinterpret_cast<const int64_t*>(field);
      writer.Int64(value_int64);
      DEB_SCAN(value_int64);
      break;
    case NdbDictionary::Column::Bigunsigned:
      value_uint64 = *reinterpret_cast<const uint64_t*>(field);
      writer.Uint64(value_uint64);
      DEB_SCAN(value_uint64);
      break;
    case NdbDictionary::Column::Float:
      value_float = *reinterpret_cast<const float*>(field);
      writer.Double(value_float);
      DEB_SCAN(value_float);
      break;
    case NdbDictionary::Column::Double:
      value_double = *reinterpret_cast<const double*>(field);
      writer.Double(value_double);
      DEB_SCAN(value_double);
      break;
    case NdbDictionary::Column::Varchar:
      varchar_len = *reinterpret_cast<const uint8_t*>(field);
      writer.String(field + 1, varchar_len);
      DEB_SCAN("[" << varchar_len << "] "
        << std::string(field + 1, varchar_len));
      break;
    case NdbDictionary::Column::Longvarchar:
      varchar_len = *reinterpret_cast<const uint16_t*>(field);
      writer.String(field + 2, varchar_len);
      DEB_SCAN("[" << varchar_len << "] "
        << std::string(field + 2, varchar_len));
      break;
    case NdbDictionary::Column::Timestamp2: {
      ///< 4 bytes + 0-3 fraction
      uint32_t precision = col->getPrecision();
      my_timeval myTV{};
      my_timestamp_from_binary(&myTV, (const unsigned char *)field, precision);
      Int64 epochIn = myTV.m_tv_sec;
      time_t stdtime(epochIn);
      struct tm *time_info = gmtime(&stdtime);
      MYSQL_TIME lTime  = {};
      lTime.year        = time_info->tm_year + 1900;
      lTime.month       = time_info->tm_mon +1;
      lTime.day         = time_info->tm_mday;
      lTime.hour        = time_info->tm_hour;
      lTime.minute      = time_info->tm_min;
      lTime.second      = time_info->tm_sec;
      lTime.second_part = myTV.m_tv_usec;
      lTime.time_type   = MYSQL_TIMESTAMP_DATETIME;
      char to[MAX_DATE_STRING_REP_LENGTH];
      memset(to, 0, MAX_DATE_STRING_REP_LENGTH);
      my_TIME_to_str(lTime, to, precision);
      std::string time_str(to);
      writer.String(time_str.data(), time_str.length());
      DEB_SCAN(time_str);
      break;
    }
    default:
      DEB_SCAN("Unexpected column type");
      writer.String("Unexpected column type");
      break;
  }
  return;
}

RS_Status CompileIndexRanges(const NdbTransaction* transaction,
                             NdbIndexScanOperation* operation,
                             const NdbRecord* index_rec,
                             IndexScanParams& index_params) {
  RS_Status status = RS_OK;
  assert(index_rec);
  int bound_num = index_params.ranges.size() * 2;
  Uint32 index_rec_len = NdbDictionary::getRecordRowLength(index_rec);
  assert(index_params.index_recs_buffer == nullptr);
  index_params.index_recs_buffer = new char[bound_num * index_rec_len];
  memset(index_params.index_recs_buffer, 0, bound_num * index_rec_len);
  char* buffer = index_params.index_recs_buffer;
  size_t buf_idx = 0;

  Uint32 range_no = 0;
  for (auto& range : index_params.ranges) {
    NdbIndexScanOperation::IndexBound bound;
    char* row_ptr = &buffer[buf_idx];
    DEB_SCAN(">>>LOWER bound: " << std::endl);
    if (range.lower != std::nullopt) {
      IndexBound& lower = range.lower.value();
      bound.low_inclusive = lower.inclusive;
      bound.low_key_count = lower.values.size();
      Uint32 curr_attrId = 0;
      Uint32 curr_pos = 0;
      bool ret = NdbDictionary::getFirstAttrId(index_rec, curr_attrId);
      assert(ret);
      for (auto& node : lower.values) {
        node.col = index_params.cols[curr_pos];
        RS_Status status = GenerateBinary(node, node.binary);
        if (status.http_code != HTTP_CODE::SUCCESS) {
          return status;
        }
        char* field = NdbDictionary::getValuePtr(index_rec, row_ptr, curr_attrId);
        DEB_SCAN("curr_pos: " << curr_pos << ", curr_attrId: " << curr_attrId
          << ", col: " << node.col->getName()
          << ", node: " << node.value.ToString()
          << ", offset: " << field - row_ptr
          << ", binary_size: " << node.binary.size()
          << std::endl);
        if (node.value.kind == Node::ParsedValue::Kind::NULLVAL) {
          Uint32 nullbit_byte_offset = 0;
          Uint32 nullbit_bit_in_byte = 0;
          ret = NdbDictionary::getNullBitOffset(index_rec, curr_attrId,
              nullbit_byte_offset,
              nullbit_bit_in_byte);
          assert(ret);
          row_ptr[nullbit_bit_in_byte] |= (1 << nullbit_bit_in_byte);
        } else {
          memcpy(field, node.binary.data(), node.binary.size());
        }
        NdbDictionary::getNextAttrId(index_rec, curr_attrId);
        curr_pos++;
      }
      bound.low_key = row_ptr;
      DEB_SCAN("low_inclusive: " << bound.low_inclusive << std::endl);
      DEB_SCAN("low_key_count: " << bound.low_key_count << std::endl);
      DEB_SCAN_BLOCK(
        std::cout << "Lower bound binary: " << std::endl;
        for (size_t i = 0; i < index_rec_len; i++) {
          unsigned char c = static_cast<unsigned char>(bound.low_key[i]);
          std::cout << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(c) << ' ';
        }
        std::cout << std::dec << std::endl;
      );
      DEB_SCAN("<<<" << std::endl);
    } else {
      bound.low_key_count = 0;
      bound.low_key = nullptr;
      bound.low_inclusive = true;
      DEB_SCAN("Empty Lower bound" << std::endl);
    }
    buf_idx += (index_rec_len);
    row_ptr = &buffer[buf_idx];

    DEB_SCAN(">>>UPPER bound: " << std::endl);
    if (range.upper != std::nullopt) {
      IndexBound& upper = range.upper.value();
      bound.high_inclusive = upper.inclusive;
      bound.high_key_count = upper.values.size();
      Uint32 curr_attrId = 0;
      Uint32 curr_pos = 0;
      bool ret = NdbDictionary::getFirstAttrId(index_rec, curr_attrId);
      assert(ret);
      for (auto& node : upper.values) {
        node.col = index_params.cols[curr_pos];
        RS_Status status = GenerateBinary(node, node.binary);
        if (status.http_code != HTTP_CODE::SUCCESS) {
          return status;
        }
        char* field = NdbDictionary::getValuePtr(index_rec, row_ptr, curr_attrId);
        DEB_SCAN("curr_pos: " << curr_pos << ", curr_attrId: " << curr_attrId
          << ", col: " << node.col->getName()
          << ", node: " << node.value.ToString()
          << ", offset: " << field - row_ptr
          << ", binary_size: " << node.binary.size()
          << std::endl);
        if (node.value.kind == Node::ParsedValue::Kind::NULLVAL) {
          Uint32 nullbit_byte_offset = 0;
          Uint32 nullbit_bit_in_byte = 0;
          ret = NdbDictionary::getNullBitOffset(index_rec, curr_attrId,
              nullbit_byte_offset,
              nullbit_bit_in_byte);
          assert(ret);
          row_ptr[nullbit_bit_in_byte] |= (1 << nullbit_bit_in_byte);
        } else {
          memcpy(field, node.binary.data(), node.binary.size());
        }
        NdbDictionary::getNextAttrId(index_rec, curr_attrId);
        curr_pos++;
      }
      bound.high_key = row_ptr;
      DEB_SCAN("high_inclusive: " << bound.high_inclusive << std::endl);
      DEB_SCAN("high_key_count: " << bound.high_key_count << std::endl);
      DEB_SCAN_BLOCK(
        std::cout << "Upper bound binary: " << std::endl;
        for (size_t i = 0; i < index_rec_len; i++) {
          unsigned char c = static_cast<unsigned char>(bound.high_key[i]);
          std::cout << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(c) << ' ';
        }
        std::cout << std::dec << std::endl;
      );
      DEB_SCAN("<<<" << std::endl);
    } else {
      bound.high_key_count = 0;
      bound.high_key = nullptr;
      bound.high_inclusive = true;
      DEB_SCAN("Empty Upper bound" << std::endl);
    }
    buf_idx += (index_rec_len);

    bound.range_no = range_no;
    range_no++;
    if (operation->setBound(index_rec, bound)) {
      RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to setBound. Error: ") +
          std::to_string(transaction->getNdbError().code) + ", " +
          std::string(transaction->getNdbError().message) +
          std::string(" Index: ") + index_params.name);
      return err;
    }
  }
  return status;
}

RS_Status perform_scan(ScanReadParams& scan_params, Ndb* ndb_object, void* json_str_buf) {
  std::string db = std::string(scan_params.path.db);
  if (ndb_object->setDatabaseName(db.c_str())) {
    RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
      std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) +
      std::string(" Database: ") +
      db);
    return err;
  }
  const NdbDictionary::Dictionary *dict = ndb_object->getDictionary();
  const NdbDictionary::Table* table = dict->getTable(scan_params.path.table.c_str());
  if (unlikely(table == nullptr)) {
    RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
      std::string(rdrsErrorMessage(ERROR_DB_TABLE_NOT_EXIST)) +
      std::string(" Database: ") + db +
      std::string(" Table: ") + scan_params.path.table);
    return err;
  }

  const NdbDictionary::Index* index = nullptr;

  std::vector<const NdbDictionary::Column*> read_columns;
  Bitmap read_set(table->getNoOfColumns());
  read_set.Init();
  bool read_cols_provided = true;
  if (scan_params.readColumns.empty()) {
    read_cols_provided = false;
    for (int i = 0; i < table->getNoOfColumns(); i++) {
      const NdbDictionary::Column *column = table->getColumn(i);
      // TODO (Zhao)
      assert(column);
      read_columns.push_back(column);
    }
  } else {
    for (const auto& col : scan_params.readColumns) {
      std::string col_name = std::string(col.column);
      const NdbDictionary::Column *column = table->getColumn(col_name.c_str());
      if (column == nullptr) {
        RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
          std::string(rdrsErrorMessage(ERROR_COLUMN_NOT_EXIST)) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table +
          std::string(" Column: ") + col_name);
        return err;
      }
      read_columns.push_back(column);
      read_set.SetBit(column->getAttrId());
    }
  }

  const NdbRecord* table_rec = table->getDefaultRecord();
  if (table_rec == nullptr) {
    RS_Status err = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
        std::string("Failed to get NdbRecord.") +
        std::string(" Database: ") + db +
        std::string(" Table: ") + scan_params.path.table);
    return err;
  }

  const NdbRecord* index_rec = nullptr;

  NdbTransaction *transaction = ndb_object->startTransaction();
  if (transaction == nullptr) {
    RS_Status err = RS_SERVER_ERROR(
        std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
        std::string("Failed to start transaction. Error: ") +
        std::to_string(transaction->getNdbError().code) + ", " +
        std::string(transaction->getNdbError().message) +
        std::string(" Database: ") + db +
        std::string(" Table: ") + scan_params.path.table);
    return err;
  }

  NdbInterpretedCode filter_code(*table_rec);
  NdbScanFilter filter(&filter_code);

  if (scan_params.filterRoot) {
    RS_Status err = BindFilterColumns(scan_params.filterRoot, table);
    if (err.http_code != HTTP_CODE::SUCCESS) {
      return err;
    }

    DEB_SCAN(std::endl);
    DEB_SCAN(">>>>>> Compiling PHYSICAL Scan Filter" << std::endl);
    if (scan_params.filterRoot->type != FilterNode::Type::LOGIC) {
      filter.begin(FilterNode::Group::AND);
      DEB_SCAN("  filter->begin(" << FilterNode::Group::AND << ")" << std::endl);
    }
    err = CompileFilter(scan_params.filterRoot, &filter);
    if (err.http_code != HTTP_CODE::SUCCESS) {
      return err;
    }
    if (scan_params.filterRoot->type != FilterNode::Type::LOGIC) {
      filter.end();
      DEB_SCAN("  filter->end()" << std::endl);
    }
    DEB_SCAN("<<<<<<" << std::endl);
  }

  NdbScanOperation::ScanOptions scan_options;
  scan_options.optionsPresent = 0;
	if (scan_params.limit < 384 /* NDBAPI DEF_BATCH_SIZE */) {
    scan_options.batch = scan_params.limit;
  }
  Uint32 scan_flags = 0;
  RS_Status status = RS_OK;

  if (scan_params.index != std::nullopt) {
    // Index scan
    IndexScanParams& index_params = scan_params.index.value();
    index = dict->getIndex(index_params.name.c_str(), *table);
    if (unlikely(index == nullptr)) {
      RS_Status err = RS_CLIENT_404_WITH_MSG_ERROR(
        std::string(rdrsErrorMessage(ERROR_INDEX_NOT_EXIST)) +
        std::string(" Database: ") + db +
        std::string(" Table: ") + scan_params.path.table +
        std::string(" Index: ") + index_params.name);
      return err;
    }

    RS_Status err = BindIndexColumns(index_params, table, index);
    if (err.http_code != HTTP_CODE::SUCCESS) {
      return err;
    }

    if (!index_params.ranges.empty()) {
      scan_flags |= (NdbScanOperation::SF_MultiRange |
                    NdbScanOperation::SF_ReadRangeNo);
    }

    if (index_params.order != IndexScanParams::Order::NO_ORDER) {
      scan_flags |= NdbScanOperation::SF_OrderBy;
      if (index_params.order == IndexScanParams::Order::DESC) {
        scan_flags |= NdbScanOperation::SF_Descending;
      }
    }
    if (scan_params.filterRoot) {
      scan_options.optionsPresent |= NdbScanOperation::ScanOptions::SO_INTERPRETED;
      scan_options.interpretedCode = &filter_code;
    }
    if (scan_flags != 0) {
      scan_options.optionsPresent |= NdbScanOperation::ScanOptions::SO_SCANFLAGS;
      scan_options.scan_flags = scan_flags;
    }

    index_rec = index->getDefaultRecord();
    NdbIndexScanOperation* operation = transaction->scanIndex(index_rec, table_rec,
        NdbOperation::LockMode::LM_CommittedRead,
        read_cols_provided ? read_set.bitmap() : nullptr,
        nullptr,
        &scan_options, sizeof(NdbScanOperation::ScanOptions));
    if (operation == nullptr) {
      RS_Status err = RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to start scanIndex operation. Error: ") +
          std::to_string(transaction->getNdbError().code) + ", " +
          std::string(transaction->getNdbError().message) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table +
          std::string(" Index: ") + index_params.name);
      return err;
    }

    if (!index_params.ranges.empty()) {
      DEB_SCAN(std::endl);
      DEB_SCAN(">>>>>> Compiling PHYSICAL index ranges" << std::endl);
      RS_Status err = CompileIndexRanges(transaction, operation,
                                         index_rec, index_params);
      if (err.http_code != HTTP_CODE::SUCCESS) {
        return err;
      }
      DEB_SCAN("<<<<<<" << std::endl);
      DEB_SCAN(std::endl);
    }

    if (transaction->execute(NdbTransaction::NoCommit) != 0) {
      RS_Status err = RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to execute transaction. Error: ") +
          std::to_string(transaction->getNdbError().code) + ", " +
          std::string(transaction->getNdbError().message) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
      return err;
    }

    Uint32 table_rec_len = NdbDictionary::getRecordRowLength(table_rec);
    assert(scan_params.table_rec_buffer == nullptr);
    scan_params.table_rec_buffer = new char[table_rec_len];
    memset(scan_params.table_rec_buffer, 0, table_rec_len);

    const char* row_ptr = scan_params.table_rec_buffer;
    int rc = 0;
    DEB_SCAN("Rows: " << std::endl);

    RJ_StringBuffer* buffer = (RJ_StringBuffer*)json_str_buf;
    RJ_Writer writer(*buffer);
    writer.StartObject();
    writer.Key("data");
    writer.StartArray();
    int rows = 0;
    while ((rc = operation->nextResult(reinterpret_cast<const char **>(&row_ptr),
            true, false)) == 0) {
      rows++;
      writer.StartObject();
      for (auto& column : read_columns) {
        writer.Key(column->getName());

        Uint32 attrId = column->getAttrId();
        Uint32 attrType = column->getType();
        DEB_SCAN("  [" << attrId << "]: ");
        bool is_null = NdbDictionary::isNull(table_rec, row_ptr, attrId);
        if (is_null) {
          DEB_SCAN("NULL");
          writer.Null();
        } else {
          const char* field = NdbDictionary::getValuePtr(table_rec, row_ptr, attrId);
          WriteColumnData2Json(writer, attrType, column, field);
        }
      }
      writer.EndObject();
      DEB_SCAN(std::endl);
      if (rows >= scan_params.limit) {
        break;
      }
    }
    writer.EndArray();
    writer.Key("rows");
    writer.Int(rows);
    writer.EndObject();

    if (rc == -1) {
      status = RS_CLIENT_404_WITH_MSG_ERROR(
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to read tuple. Error: ") +
          std::to_string(transaction->getNdbError().code) + ", " +
          std::string(transaction->getNdbError().message) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
    }
  } else {
    // Table scan
    NdbScanOperation* operation = nullptr;
    if (scan_params.filterRoot) {
      scan_options.optionsPresent = NdbScanOperation::ScanOptions::SO_INTERPRETED;
      scan_options.interpretedCode = &filter_code;

      operation = transaction->scanTable(table_rec,
          NdbOperation::LockMode::LM_CommittedRead,
          read_cols_provided ? read_set.bitmap() : nullptr,
          &scan_options, sizeof(NdbScanOperation::ScanOptions));
    } else {
      operation = transaction->scanTable(table_rec,
          NdbOperation::LockMode::LM_CommittedRead,
          read_cols_provided ? read_set.bitmap() : nullptr,
          nullptr, 0);
    }
    if (operation == nullptr) {
      RS_Status err = RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to start scanTable operation. Error: ") +
          std::to_string(transaction->getNdbError().code) + ", " +
          std::string(transaction->getNdbError().message) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
      return err;
    }

    if (transaction->execute(NdbTransaction::NoCommit) != 0) {
      RS_Status err = RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to execute transaction. Error: ") +
          std::to_string(transaction->getNdbError().code) + ", " +
          std::string(transaction->getNdbError().message) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
      return err;
    }


    Uint32 table_rec_len = NdbDictionary::getRecordRowLength(table_rec);
    scan_params.table_rec_buffer = new char[table_rec_len];
    memset(scan_params.table_rec_buffer, 0, table_rec_len);

    const char* row_ptr = scan_params.table_rec_buffer;
    int rc = 0;
    DEB_SCAN("Rows: " << std::endl);

    RJ_StringBuffer* buffer = (RJ_StringBuffer*)json_str_buf;
    RJ_Writer writer(*buffer);
    writer.StartObject();
    writer.Key("data");
    writer.StartArray();
    int rows = 0;
    while ((rc = operation->nextResult(reinterpret_cast<const char **>(&row_ptr),
            true, false)) == 0) {
      rows++;
      writer.StartObject();
      for (auto& column : read_columns) {
        writer.Key(column->getName());

        Uint32 attrId = column->getAttrId();
        Uint32 attrType = column->getType();
        DEB_SCAN("  [" << attrId << "]: ");
        bool is_null = NdbDictionary::isNull(table_rec, row_ptr, attrId);
        if (is_null) {
          DEB_SCAN("NULL");
          writer.Null();
        } else {
          const char* field = NdbDictionary::getValuePtr(table_rec, row_ptr, attrId);
          WriteColumnData2Json(writer, attrType, column, field);
        }
      }
      writer.EndObject();
      DEB_SCAN(std::endl);
      if (rows >= scan_params.limit) {
        break;
      }
    }
    writer.EndArray();
    writer.Key("rows");
    writer.Int(rows);
    writer.EndObject();

    RS_Status status = RS_OK;
    if (rc == -1) {
      status = RS_SERVER_ERROR(
          std::string(rdrsErrorMessage(ERROR_SCAN_OPERATION_FAILED)) +
          std::string("Failed to read tuple. Error: ") +
          std::to_string(transaction->getNdbError().code) + ", " +
          std::string(transaction->getNdbError().message) +
          std::string(" Database: ") + db +
          std::string(" Table: ") + scan_params.path.table);
    }
    operation->close();
  }
  ndb_object->closeTransaction(transaction);

  return status;
}

RS_Status scan_read(ScanReadParams& scan_params, unsigned int threadIndex, void* doc) {
  Ndb *ndb_object  = nullptr;
  RS_Status status = rdrsRonDBConnectionPool->GetNdbObject(&ndb_object,
                                                           threadIndex);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  DATA_OP_RETRY_HANDLER(
    status = perform_scan(scan_params, ndb_object, doc);
  )
  rdrsRonDBConnectionPool->ReturnNdbObject(ndb_object,
                                           &status,
                                           threadIndex);
  return status;
}
