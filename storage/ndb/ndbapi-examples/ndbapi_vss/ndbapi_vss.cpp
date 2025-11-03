/*
   Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifdef _WIN32
#include <winsock2.h>
#endif
#include <mysql.h>
#include <mysqld_error.h>
#include <NdbApi.hpp>
// Used for cout
#include<iomanip>
#include <cassert>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ndb_config.h>
#include <random>
#include <fstream>
#include <AttributeHeader.hpp>
#include <NdbSleep.h>

#include <queue>
#include <chrono>
#include <simsimd/simsimd.h>


/**
 * Helper debugging macros
 */
#define PRINT_ERROR(code,msg) \
  std::cout << "Error in " << __FILE__ << ", line: " << __LINE__ \
  << ", code: " << code \
  << ", msg: " << msg << "." << std::endl
#define MYSQLERROR(mysql) { \
  PRINT_ERROR(mysql_errno(&mysql),mysql_error(&mysql)); \
  exit(-1); }
#define APIERROR(error) { \
  PRINT_ERROR(error.code,error.message); \
  exit(-1); }

std::random_device rd;
std::mt19937 gen(rd());

#define DIMS 1024
#define VEC_TOP_N 10
int scan_vector_search(Ndb * myNdb, MYSQL& mysql, bool validation)
{
  int                  retryAttempt = 0;
  const int            retryMax = 10;
  NdbError              err;
  NdbTransaction	*myTrans;
  NdbScanOperation	*myScanOp;

  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("vec_tbl");

  if (myTable == NULL)
    APIERROR(myDict->getNdbError());
  while (true)
  {

    if (retryAttempt >= retryMax)
    {
      std::cout << "ERROR: has retried this operation " << retryAttempt
        << " times, failing!" << std::endl;
      return -1;
    }

    myTrans = myNdb->startTransaction();
    if (myTrans == NULL)
    {
      const NdbError err = myNdb->getNdbError();

      if (err.status == NdbError::TemporaryError)
      {
        NdbSleep_MilliSleep(50);
        retryAttempt++;
        continue;
      }
      std::cout << err.message << std::endl;
      return -1;
    }
    /*
     * Define a scan operation.
     * NDBAPI.
     */
    myScanOp = myTrans->getNdbScanOperation(myTable);
    if (myScanOp == NULL)
    {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->readTuples(NdbOperation::LM_CommittedRead) != 0) {
      APIERROR (myTrans->getNdbError());
    }

    /* Filter pk <= 100 */
    // Uint32 val = 100;
    // NdbScanFilter filter(myScanOp);
    // if (filter.begin(NdbScanFilter::AND) < 0  ||
    //     filter.cmp(NdbScanFilter::COND_LT, 0, &val, sizeof(val)) < 0 ||
    //     filter.end() < 0) {
    //   std::cout <<  myTrans->getNdbError().message << std::endl;
    //   myNdb->closeTransaction(myTrans);
    //   return -1;
    // }
    
    NdbRecAttr* myRecAttr[2];
    myRecAttr[0] = myScanOp->getValue("pk");
    myRecAttr[1] = myScanOp->getValue("val");
    if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
    }

    /*
     * Define the target vector
     */
    float vec[DIMS];
    for (int i = 0; i < DIMS; i++) {
      vec[i] = 0.5;
    }

    NdbAggregator aggregator(myTable);
    bool ret = aggregator.VectorSearch("vec", vec, DIMS, VEC_TOP_N);
    assert(ret);
    if (myScanOp->setAggregationCode(&aggregator) == -1) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }
    auto start = std::chrono::high_resolution_clock::now();
    if (myScanOp->DoVectorSearch(myRecAttr, 2) == -1) {
      err = myTrans->getNdbError();
      std::cout << "DoVectorSearch failed: " << err.message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    fprintf(stderr, "------FINAL RESULT------\n");
    while (aggregator.VecFetchNextResult()) {
      fprintf(stderr, "pk: %d, val: %d\n",
          myRecAttr[0]->int32_value(),
          myRecAttr[1]->int32_value());
    }

    std::chrono::duration<double, std::milli> elapsed = end - start;
    std::cout << "Time cost: " << elapsed.count() << " ms" << std::endl;

    myNdb->closeTransaction(myTrans);
    return 1;
  }
  return -1;
}

int scan_index_vector_search(Ndb *myNdb, MYSQL& mysql, bool validation) {
  NdbError              err;
  NdbDictionary::Dictionary* myDict = myNdb->getDictionary();
  const NdbDictionary::Index *myPIndex = myDict->getIndex("index_val", "vec_tbl");
  if (myPIndex == NULL) {
    APIERROR(myDict->getNdbError());
  }

  NdbTransaction *myTrans = myNdb->startTransaction();
  if (myTrans == NULL) {
    APIERROR(myNdb->getNdbError());
  }

  NdbIndexScanOperation *myIndexScanOp = myTrans->getNdbIndexScanOperation(myPIndex);


  /* Index Scan */
  Uint32 scanFlags= NdbScanOperation::SF_OrderBy |
                    NdbScanOperation::SF_MultiRange;
  /**
   * Read without locks, without being placed in lock queue
   */
  if (myIndexScanOp->readTuples(NdbOperation::LM_CommittedRead,
                                scanFlags
                                /*(Uint32) 0 // batch */
                                /*(Uint32) 0 // parallel */
                                ) != 0) {
    APIERROR (myTrans->getNdbError());
  }

  /* Index range: val >= 10000 and val < 100000 */
  Uint32 low=10000;
  Uint32 high=100000;

  if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundLE, (char*)&low)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->setBound("val", NdbIndexScanOperation::BoundGT, (char*)&high)) {
    APIERROR(myTrans->getNdbError());
  }
  if (myIndexScanOp->end_of_bound(0)) {
    APIERROR(myIndexScanOp->getNdbError());
  }

  /* Filter: pk < 500 */
  Uint32 val = 500;
  NdbScanFilter filter(myIndexScanOp);
  if (filter.begin(NdbScanFilter::AND) < 0  ||
      filter.cmp(NdbScanFilter::COND_LT, 0, &val, sizeof(val)) < 0 ||
      filter.end() < 0) {
    std::cout <<  myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }
  NdbRecAttr* myRecAttr[2];
  myRecAttr[0] = myIndexScanOp->getValue("pk");
  myRecAttr[1] = myIndexScanOp->getValue("val");
  if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr) {
    std::cout << myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
  }

  /*
   * Define the target vector
   */
  float vec[DIMS];
  for (int i = 0; i < DIMS; i++) {
    vec[i] = 0.5;
  }

  const NdbDictionary::Table *myTable= myDict->getTable("vec_tbl");
  if (myTable == NULL) {
    APIERROR(myDict->getNdbError());
  }

  NdbAggregator aggregator(myTable);
  bool ret = aggregator.VectorSearch("vec", vec, DIMS, VEC_TOP_N);
  assert(ret);
  if (myIndexScanOp->setAggregationCode(&aggregator) == -1) {
    std::cout << myTrans->getNdbError().message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }
  auto start = std::chrono::high_resolution_clock::now();
  if (myIndexScanOp->DoVectorSearch(myRecAttr, 2) == -1) {
    err = myTrans->getNdbError();
    std::cout << "DoVectorSearch failed: " << err.message << std::endl;
    myNdb->closeTransaction(myTrans);
    return -1;
  }
  auto end = std::chrono::high_resolution_clock::now();

  fprintf(stderr, "------FINAL RESULT------\n");
  while (aggregator.VecFetchNextResult()) {
    fprintf(stderr, "pk: %d, val: %d\n",
        myRecAttr[0]->int32_value(),
        myRecAttr[1]->int32_value());
  }
  std::chrono::duration<double, std::milli> elapsed = end - start;
  std::cout << "Time cost: " << elapsed.count() << " ms" << std::endl;

  myNdb->closeTransaction(myTrans);
  return 1;
}

int scan_regular_vector_search(Ndb * myNdb, MYSQL& mysql, bool validation)
{
  // Scan all records exclusive and update
  // them one by one
  int                  retryAttempt = 0;
  const int            retryMax = 10;
  NdbError              err;
  NdbTransaction	*myTrans;
  NdbScanOperation	*myScanOp;

  const NdbDictionary::Dictionary* myDict= myNdb->getDictionary();
  const NdbDictionary::Table *myTable= myDict->getTable("vec_tbl");

  if (myTable == NULL)
    APIERROR(myDict->getNdbError());
  while (true)
  {

    if (retryAttempt >= retryMax)
    {
      std::cout << "ERROR: has retried this operation " << retryAttempt
        << " times, failing!" << std::endl;
      return -1;
    }

    myTrans = myNdb->startTransaction();
    if (myTrans == NULL)
    {
      const NdbError err = myNdb->getNdbError();

      if (err.status == NdbError::TemporaryError)
      {
        NdbSleep_MilliSleep(50);
        retryAttempt++;
        continue;
      }
      std::cout << err.message << std::endl;
      return -1;
    }
    /*
     * Define a scan operation.
     * NDBAPI.
     */
    myScanOp = myTrans->getNdbScanOperation(myTable);
    if (myScanOp == NULL)
    {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
      return -1;
    }

    if (myScanOp->readTuples(NdbOperation::LM_CommittedRead) != 0) {
      APIERROR (myTrans->getNdbError());
    }

    NdbRecAttr* myRecAttr[3];
    myRecAttr[0] = myScanOp->getValue("pk");
    myRecAttr[1] = myScanOp->getValue("val");
    myRecAttr[2] = myScanOp->getValue("vec");
    if (myRecAttr[0] == nullptr || myRecAttr[1] == nullptr || myRecAttr[2] == nullptr) {
      std::cout << myTrans->getNdbError().message << std::endl;
      myNdb->closeTransaction(myTrans);
    }

    /*
     * Define the target vector
     */
    float vec[DIMS];
    for (int i = 0; i < DIMS; i++) {
      vec[i] = 0.5;
    }

    auto start = std::chrono::high_resolution_clock::now();
    if (myTrans->execute(NdbTransaction::NoCommit) != 0) {
      return -1;
    }

    std::priority_queue<NdbAggregator::VectorSearchResult*,
      std::vector<NdbAggregator::VectorSearchResult*>,
      NdbAggregator::ByDistance> vec_results;
    int check = -1;
    double distance = 0;
    int count = 0;
    while ((check = myScanOp->nextResult(true)) == 0) {
      count++;
      // Uint16 len = *(Uint16*)(myRecAttr[2]->aRef());
      float* current = (float*)((char*)(myRecAttr[2]->aRef()) + 2);
      simsimd_l2sq_f32(current, vec, DIMS, &distance);
      if (vec_results.size() < VEC_TOP_N ||
          distance < vec_results.top()->distance_) {
        NdbAggregator::VectorSearchResult* candidate =
          new NdbAggregator::VectorSearchResult(distance, 2, myRecAttr);
        vec_results.push(candidate);
        if (vec_results.size() > VEC_TOP_N) {
          NdbAggregator::VectorSearchResult* kickout = vec_results.top();
          vec_results.pop();
          delete kickout;
        }
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> elapsed = end - start;
    fprintf(stderr, "------FINAL RESULT------\n");
    std::vector<NdbAggregator::VectorSearchResult*> vec_results_final;
    while (!vec_results.empty()) {
      vec_results_final.push_back(vec_results.top());
      vec_results.pop();
    }
    std::for_each(vec_results_final.rbegin(), vec_results_final.rend(),
        [](NdbAggregator::VectorSearchResult* candidate) {
          std::cout << "pk: " << candidate->attrs_[0]->int32_value();
          std::cout << ", val: " << candidate->attrs_[1]->int32_value() << std::endl;
          delete candidate;
        });
    std::cout << "Time cost: " << elapsed.count() << " ms" << std::endl;

    myNdb->closeTransaction(myTrans);
    return 1;
  }
  return -1;
}

void ndb_run_scan(const char * connectstring, MYSQL& mysql,
                  bool load, bool populate_data, bool validation)
{

  /**************************************************************
   * Connect to ndb cluster                                     *
   **************************************************************/

  Ndb_cluster_connection cluster_connection(connectstring);
  if (cluster_connection.connect(4, 5, 1))
  {
    std::cout << "Unable to connect to cluster within 30 secs." << std::endl;
    exit(-1);
  }
  // Optionally connect and wait for the storage nodes (ndbd's)
  if (cluster_connection.wait_until_ready(30,0) < 0)
  {
    std::cout << "Cluster was not ready within 30 secs.\n";
    exit(-1);
  }

  Ndb myNdb(&cluster_connection,"test_ndb_vec");
  if (myNdb.init(1024) == -1) {      // Set max 1024  parallel transactions
    APIERROR(myNdb.getNdbError());
    exit(-1);
  }

  int i = 0;
  while (i < 10) {
    fprintf(stderr, "1. Pushdown Vector Search via TABLE Scan\n");
    fprintf(stderr, "  SELECT pk, val FROM vec_tbl\n");
    fprintf(stderr, "                 ORDER BY embedding <-> '[0.5, 0.5, ...]'::vector\n");
    fprintf(stderr, "                 LIMIT %u;\n", VEC_TOP_N);
    if(scan_vector_search(&myNdb, mysql, validation) > 0) {
      std::cout << "Query 1: success!" << std::endl  << std::endl;
    }

    fprintf(stderr, "2. Non-pushdown Vector Search via TABLE Scan\n");
    fprintf(stderr, "  SELECT pk, val FROM vec_tbl\n");
    fprintf(stderr, "                 ORDER BY embedding <-> '[0.5, 0.5, ...]'::vector\n");
    fprintf(stderr, "                 LIMIT %u;\n", VEC_TOP_N);
    if(scan_regular_vector_search(&myNdb, mysql, validation) > 0) {
      std::cout << "Query 2: success!" << std::endl  << std::endl;
    }

    fprintf(stderr, "3. Pushdown Vector Search via Index Scan with Lower–Upper Bounds and Filter\n");
    fprintf(stderr, "  SELECT pk, val FROM vec_tbl\n");
    fprintf(stderr, "                 WHERE val >= 10000 AND val < 100000 AND pk < 500\n");
    fprintf(stderr, "                 ORDER BY embedding <-> '[0.5, 0.5, ...]'::vector\n");
    fprintf(stderr, "                 LIMIT %u;\n", VEC_TOP_N);
    if(scan_index_vector_search(&myNdb, mysql, validation) > 0) {
      std::cout << "Query 3: success!" << std::endl  << std::endl;
    }
    i++;
    sleep(1);
  }

}

int main(int argc, char** argv)
{
  // char * mysqld_sock  = argv[1];
  const char *connectstring = argv[2];
  MYSQL mysql;

  mysql_init(& mysql);

  ndb_init();
  bool load = false;
  bool populate = false;
  bool validation = false;
  ndb_run_scan(connectstring, mysql, load, populate, validation);
  ndb_end(0);

  mysql_close(&mysql);

  return 0;
}
