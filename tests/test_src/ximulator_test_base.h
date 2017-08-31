//
// Created by Xin Huang on 3/14/17.
//

#ifndef XIMULATOR_XIMULATOR_TEST_BASE_H_H
#define XIMULATOR_XIMULATOR_TEST_BASE_H_H

#include "gtest/gtest.h"
#include "src/traffic_generator.h"
#include "src/db_logger.h"

class Coflow;

class XimulatorTestBase : public ::testing::Test {
 protected:
  XimulatorTestBase() : db_logger_("test") {

    // TODO: replace MAC_BASE_DIR with the absolute path to your directory of the
    // simulator package, e.g. "/Users/yourid/Documents/research/2D-Placement/"
    string MAC_BASE_DIR = "/Users/yourid/Documents/research/2D-Placement/";
    string LINUX_BASE_DIR = "../";
    string BASE_DIR = IsOnApple() ? MAC_BASE_DIR : LINUX_BASE_DIR;

    string TEST_DATA_DIR = BASE_DIR + "tests/test_data/";
    TRAFFIC_TRACE_FILE_NAME = TEST_DATA_DIR + "test_trace.txt";
    TRAFFIC_AUDIT_FILE_NAME = TEST_DATA_DIR + "audit_traffic.txt";

    TEST_DATA_DIR_ = TEST_DATA_DIR;
  }
  string TEST_DATA_DIR_;
  // db_logger_ may be shared by many many test and writes junks into the db.
  DbLogger db_logger_;
};
#endif //XIMULATOR_XIMULATOR_TEST_BASE_H_H
