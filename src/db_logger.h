#ifndef XIMULATOR_DB_LOGGER_H
#define XIMULATOR_DB_LOGGER_H

#include <iostream>

#include "coflow.h"

using namespace std;

class DbLogger {
 public:
  DbLogger(string table_subfix = "");
  ~DbLogger();
  // add subfix to all tables exported to the database.
  void SetResultSubfix(string subfix) {
    table_subfix_ = subfix;
  }
  // Write one entry for each coflow. Each entry contains metrics on coflow
  // characteristics. No simulation is needed to obtian these metrics.
  void WriteCoflowFeatures(Coflow *coflow);
  // Called when the coflow/flow is finished. Calculate some metrics on
  // scheduling performance, and log to database.
  void WriteOnCoflowFinish(double finish_time, Coflow *coflow);
  void WriteOnFlowFinish(double finish_time, Flow *flow);

 private:
  bool Connect();
  // return true if table is dropped successfully.
  bool DropIfExist(string table_name);
  // schema
  bool has_init_coflow_info_table_;
  void InitCoflowInfoTable();

  static bool LOG_FLOW_INFO_;
  void InitFlowInfoTable();

  string table_subfix_;
  string GetTableName(string table_type);
};

#endif //XIMULATOR_DB_LOGGER_H
