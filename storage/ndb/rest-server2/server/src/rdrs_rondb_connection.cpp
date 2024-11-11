/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
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

#include "rdrs_rondb_connection.hpp"
#include "status.hpp"
#include "error_strings.h"
#include "logger.hpp"

#include <cstddef>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <NdbThread.h>
#include <ndb_global.h>
#include <util/require.h>

RDRSRonDBConnection::RDRSRonDBConnection(const char *connection_string,
                                         Uint32 node_id,
                                         Uint32 connection_retries,
                                         Uint32 connection_retry_delay_in_sec) {

  connectionMutex = NdbMutex_Create();
  connectionInfoMutex = NdbMutex_Create();
  stats.ndb_objects_available = 0;
  stats.ndb_objects_count = 0;
  stats.ndb_objects_created = 0;
  stats.ndb_objects_deleted = 0;
  stats.is_reconnection_in_progress = false;
  stats.is_shutdown = false;
  stats.is_shutting_down = false;
  stats.connection_state = DISCONNECTED;

  size_t connection_string_len = strlen(connection_string);
  this->connection_string =
    reinterpret_cast<char *>(malloc(connection_string_len + 1));
  std::strncpy(this->connection_string,
               connection_string,
               connection_string_len + 1);
  this->connection_string[connection_string_len] = '\0';
  this->m_node_id = node_id;
  this->connection_retries = connection_retries;
  this->connection_retry_delay_in_sec = connection_retry_delay_in_sec;

  ndbConnection = nullptr;
  reconnectionThread = nullptr;
}

RS_Status RDRSRonDBConnection::Connect() {

  rdrs_logger::info(std::string("Connecting to ") + connection_string);
  {
    NdbMutex_Lock(connectionInfoMutex);
    if (unlikely(stats.is_shutdown || stats.is_shutting_down)) {
      NdbMutex_Unlock(connectionInfoMutex);
      return RS_SERVER_ERROR(ERROR_034);
    }
    require(stats.connection_state != CONNECTED);
    NdbMutex_Unlock(connectionInfoMutex);
  }
  {
    NdbMutex_Lock(connectionMutex);
    require(ndbConnection == nullptr);
    int retCode = 0;
    ndbConnection = new Ndb_cluster_connection(connection_string, m_node_id);
    retCode = ndbConnection->connect(connection_retries,
                                     connection_retry_delay_in_sec,
                                     0);
    if (unlikely(retCode != 0)) {
      NdbMutex_Unlock(connectionMutex);
      return RS_SERVER_ERROR(
        ERROR_002 + std::string(" RetCode: ") + std::to_string(retCode));
    }
    retCode = ndbConnection->wait_until_ready(30, 30);
    if (retCode != 0) {
      RS_Status status = RS_SERVER_ERROR(
        ERROR_003 + std::string(" RetCode: ") + std::to_string(retCode) +
        std::string(" Lastest Error: ") +
        std::to_string(ndbConnection->get_latest_error()) +
        std::string(" Lastest Error Msg: ") +
        std::string(ndbConnection->get_latest_error_msg()));
      NdbMutex_Unlock(connectionMutex);
      return status;
    }
    NdbMutex_Unlock(connectionMutex);
  }
  {
    NdbMutex_Lock(connectionInfoMutex);
    stats.connection_state = CONNECTED;
    NdbMutex_Unlock(connectionInfoMutex);
  }
  rdrs_logger::info("RonDB connection and object pool initialized");
  return RS_OK;
}

RDRSRonDBConnection::~RDRSRonDBConnection() {
  NdbMutex_Destroy(connectionMutex);
  NdbMutex_Destroy(connectionInfoMutex);
}

RS_Status RDRSRonDBConnection::GetNdbObject(Ndb **ndb_object) {

  {
    bool is_shutdown;
    bool reconnection_in_progress;
    STATE connection_state = DISCONNECTED;
    {
      NdbMutex_Lock(connectionInfoMutex);
      is_shutdown = stats.is_shutdown || stats.is_shutting_down;
      reconnection_in_progress = stats.is_reconnection_in_progress;
      connection_state = stats.connection_state;
      NdbMutex_Unlock(connectionInfoMutex);
    }
    if (unlikely(is_shutdown)) {
      rdrs_logger::error(ERROR_034);
      return RS_SERVER_ERROR(ERROR_034);
    }
    if (unlikely(connection_state != CONNECTED)) {
      if (!reconnection_in_progress) {
        // If previous reconnection attempts have failed then
        // restart the reconnection process
        rdrs_logger::debug("GetNdbObject triggered reconnection");
        Reconnect();
      }
      rdrs_logger::warn(ERROR_033 + std::string(" Connection State: ") +
                           std::to_string(connection_state) +
                           std::string(" Reconnection State: ") +
                           std::to_string(reconnection_in_progress));
      return RS_SERVER_ERROR(ERROR_033);
    }
  }
  {
    NdbMutex_Lock(connectionMutex);
    NdbMutex_Lock(connectionInfoMutex);
    RS_Status ret_status = RS_OK;
    if (unlikely(availableNdbObjects.empty())) {
      *ndb_object = new Ndb(ndbConnection);
      int retCode = (*ndb_object)->init();
      if (unlikely(retCode != 0)) {
        delete ndb_object;
        ret_status =
            RS_SERVER_ERROR(
              ERROR_004 + std::string(" RetCode: ") + std::to_string(retCode));
      }
      stats.ndb_objects_created++;
      stats.ndb_objects_count++;
      allAvailableNdbObjects.push_back(*ndb_object);
    } else {
      *ndb_object = availableNdbObjects.front();
      availableNdbObjects.pop_front();
    }
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    return ret_status;
  }
}

void RDRSRonDBConnection::ReturnNDBObjectToPool(Ndb *ndb_object,
                                                RS_Status *status) {
  {
    NdbMutex_Lock(connectionMutex);
    availableNdbObjects.push_back(ndb_object);
    NdbMutex_Unlock(connectionMutex);
  }
  // Note there are no unit test for this
  // In order to test this run the TestReconnection1 for longer duration
  // and then drop the ndbconnection using iptables or by disconnection
  // the network.
  if (unlikely(status != nullptr && status->http_code != SUCCESS)) {
    // Classification.UnknownResultError is the classification
    // for loss of connectivity to the cluster
    if (status->classification == NdbError::UnknownResultError) {
      rdrs_logger::error(
        "Detected connection loss. Triggering reconnection.");
      Reconnect();
    }
  }
}

void RDRSRonDBConnection::GetStats(RonDB_Stats &ret) {
  NdbMutex_Lock(connectionInfoMutex);
  stats.ndb_objects_available = availableNdbObjects.size();
  ret = stats;
  NdbMutex_Unlock(connectionInfoMutex);
  return;
}

RS_Status RDRSRonDBConnection::Shutdown(bool end) {

  // wait for all NDB objects to return
  using namespace std::chrono;
  Int64 startTime =
    duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
  Int64 timeElapsed = 0;

  // We are shutting down for good
  if (end) {
    NdbMutex_Lock(connectionInfoMutex);
    stats.is_shutting_down = true;
    NdbMutex_Unlock(connectionInfoMutex);
  }

  bool allNDBObjectsCountedFor = false;
  do {
    size_t expectedSize = 0;
    Uint32 sizeGot = 0;
    {
      NdbMutex_Lock(connectionMutex);
      sizeGot      = availableNdbObjects.size();
      expectedSize = stats.ndb_objects_created;
      NdbMutex_Unlock(connectionMutex);
    }

    if (expectedSize != sizeGot) {
      rdrs_logger::warn(
        "Waiting to all NDB objects to return before shutdown."
        " Expected Size: " + std::to_string(expectedSize) +
        " Have: " + std::to_string(sizeGot));
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
    } else {
      allNDBObjectsCountedFor = true;
      break;
    }
    timeElapsed =
      duration_cast<milliseconds>(
      system_clock::now().time_since_epoch()).count() - startTime;
  } while (timeElapsed < 120 * 1000);

  if (!allNDBObjectsCountedFor) {
    rdrs_logger::error("Timedout waiting for all NDB objects.");
  } else {
    rdrs_logger::info(
      "All NDB objects are accounted for. Total objects: " +
      std::to_string(stats.ndb_objects_created));
  }
  rdrs_logger::info("Shutting down RonDB connection and NDB object pool");
  {
    NdbMutex_Lock(connectionInfoMutex);
    stats.connection_state = DISCONNECTED;
    NdbMutex_Unlock(connectionInfoMutex);
  }
  {
    NdbMutex_Lock(connectionMutex);
    NdbMutex_Lock(connectionInfoMutex);
    // Delete all Ndb objects
    while (allAvailableNdbObjects.size() > 0) {
      Ndb *ndb_object = allAvailableNdbObjects.front();
      allAvailableNdbObjects.pop_front();
      delete ndb_object;
    }
    availableNdbObjects.clear();
    allAvailableNdbObjects.clear();

    // Clean up stats
    stats.ndb_objects_available = 0;
    stats.ndb_objects_count = 0;
    stats.ndb_objects_created = 0;
    stats.ndb_objects_deleted = 0;
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
  }
  {
    NdbMutex_Lock(connectionMutex);
    // Delete connection
    try {
      rdrs_logger::debug("delete ndbconnection");
      delete ndbConnection;
    } catch (...) {
      rdrs_logger::warn("Exception in Shutdown");
    }
    ndbConnection = nullptr;
    NdbMutex_Unlock(connectionMutex);
  }
  {
    NdbMutex_Lock(connectionMutex);
    NdbMutex_Lock(connectionInfoMutex);
    if (end) {
      stats.is_shutdown = true;
      stats.is_shutting_down = false;
      free(connection_string);
      if (reconnectionThread != nullptr) {
        NdbThread_Destroy(&reconnectionThread);
        reconnectionThread = nullptr;
      }
    }
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    rdrs_logger::info("RonDB connection and NDB object pool shutdown");
    return RS_OK;
  }
}

RS_Status RDRSRonDBConnection::ReconnectHandler() {

  {
    NdbMutex_Lock(connectionInfoMutex);
    require(stats.is_reconnection_in_progress);
    NdbMutex_Unlock(connectionInfoMutex);
  }
  RS_Status status = Shutdown(false);
  if (status.http_code != SUCCESS) {
    NdbMutex_Lock(connectionInfoMutex);
    stats.is_reconnection_in_progress = false;
    NdbMutex_Unlock(connectionInfoMutex);
    return RS_SERVER_ERROR(
      "Reconnection. Shutdown failed. " + std::string("code: ") +
      std::to_string(status.code) + std::string(" Classification: ") +
      std::to_string(status.classification) + std::string(" Msg: ") +
      std::string(status.message));
  }
  status = Connect();
  if (status.http_code != SUCCESS) {
    NdbMutex_Lock(connectionInfoMutex);
    stats.is_reconnection_in_progress = false;
    NdbMutex_Unlock(connectionInfoMutex);
    return RS_SERVER_ERROR(
      "Reconnection. Connection failed. " + std::string("code: ") +
      std::to_string(status.code) + std::string(" Classification: ") +
      std::to_string(status.classification) + std::string(" Msg: ") +
      std::string(status.message));
  }
  NdbMutex_Lock(connectionInfoMutex);
  stats.is_reconnection_in_progress = false;
  NdbMutex_Unlock(connectionInfoMutex);
  return RS_OK;
}

static void *reconnect_thread_wrapper(void *arg) {
  rdrs_logger::info("Reconnection thread has started running.");
  RDRSRonDBConnection *rdrsRonDBConnection = (RDRSRonDBConnection *)arg;
  rdrsRonDBConnection->ReconnectHandler();
  return NULL;
}

// Note it is only public for testing
RS_Status RDRSRonDBConnection::Reconnect() {

  NdbMutex_Lock(connectionMutex);
  NdbMutex_Lock(connectionInfoMutex);
  if (stats.is_reconnection_in_progress) {
    NdbMutex_Unlock(connectionMutex);
    NdbMutex_Unlock(connectionInfoMutex);
    rdrs_logger::info(
      "Ignoring RonDB reconnection request. A reconnection request is"
      " already in progress");
    return RS_SERVER_ERROR(ERROR_036);
  }
  stats.is_reconnection_in_progress = true;
  // clean previous failed/completed reconnection thread
  if (reconnectionThread != nullptr) {
    NdbThread_Destroy(&reconnectionThread);
    reconnectionThread = nullptr;
  }
  reconnectionThread = NdbThread_Create(reconnect_thread_wrapper,
                                        (NDB_THREAD_ARG *)this,
                                        0, // default stack size
                                        "reconnection_thread",
                                        NDB_THREAD_PRIO_MEAN);
  if (reconnectionThread == nullptr) {
    rdrs_logger::error("Failed to start reconnection thread");
  }
  NdbMutex_Unlock(connectionMutex);
  NdbMutex_Unlock(connectionInfoMutex);
  return RS_OK;
}
