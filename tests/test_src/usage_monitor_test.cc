//
// Created by Xin Huang on 3/13/17.
//

#include "gtest/gtest.h"
#include "src/usage_monitor.h"
#include "ximulator_test_base.h"

class UsageMonitorTest : public XimulatorTestBase {
 protected:

  virtual void TearDown() {}

  virtual void SetUp() {}

  void LoadAllCoflows(std::vector<Coflow *> &coflows) {
    AnalyzeBase analyzer(&db_logger_);
    analyzer.LoadAllCoflows(&coflows);
  }
};

TEST_F(UsageMonitorTest, RegisterAndUnregisterAllCoflows) {
  DbLogger db_logger("test"/*table_subfix*/);
  UsageMonitor usage_monitor(&db_logger);

  vector<Coflow *> coflows;
  LoadAllCoflows(coflows);

  for (Coflow *coflow : coflows) {
    usage_monitor.Register(coflow->GetStartTime(), coflow);
  }

  double dummy_time = 0.1;
  double dummy_time_interval = 0.03;
  for (Coflow *coflow : coflows) {
    for (Flow *flow : *coflow->GetFlows()) {
      dummy_time += dummy_time_interval;
      usage_monitor.Unregister(dummy_time, coflow, flow);
    }
    usage_monitor.Unregister(dummy_time, coflow);
  }
}

TEST_F(UsageMonitorTest, AnalyzeAllCoflows) {
  DbLogger db_logger("test"/*table_subfix*/);

  vector<Coflow *> coflows;
  LoadAllCoflows(coflows);

  for (Coflow *coflow : coflows) {
    db_logger.WriteCoflowFeatures(coflow);
  }
}

TEST_F(UsageMonitorTest, FlowQueue) {
  DbLogger db_logger("test"/*table_subfix*/);
  UsageMonitor usage_monitor(&db_logger);

  vector<Coflow *> coflows;
  LoadAllCoflows(coflows);

  for (Coflow *coflow : coflows) {
    usage_monitor.Register(coflow->GetStartTime(), coflow);
    map<int, vector<Flow *>> local_flow_queus = usage_monitor.GetIOFlowQueues();
    switch (coflow->GetJobId()) {
      case 18: //
        ASSERT_EQ(1, local_flow_queus[46].size());
        EXPECT_EQ(8578624, local_flow_queus[46][0]->GetSizeInBit());
        break;
      case 23: //
        ASSERT_EQ(2, local_flow_queus[46].size());
        EXPECT_EQ(8000000, local_flow_queus[46][0]->GetSizeInBit());
        EXPECT_EQ(8578624, local_flow_queus[46][1]->GetSizeInBit());
        break;
      default:break;
    }
  }

  double dummy_time = 0.1;
  double dummy_time_interval = 0.03;
  for (Coflow *coflow : coflows) {
    for (Flow *flow : *coflow->GetFlows()) {
      dummy_time += dummy_time_interval;
      usage_monitor.Unregister(dummy_time, coflow, flow);
    }
    usage_monitor.Unregister(dummy_time, coflow);
    map<int, vector<Flow *>> local_flow_queus = usage_monitor.GetIOFlowQueues();
    switch (coflow->GetJobId()) {
      case 18: //
        ASSERT_EQ(1, local_flow_queus[46].size());
        EXPECT_EQ(8000000, local_flow_queus[46][0]->GetSizeInBit());
        break;
      case 23: //
        EXPECT_EQ(0, local_flow_queus[46].size());
        break;
      default:break;
    }
  }
}