//
// Created by Xin Huang on 3/5/17.
//

#include <memory>

#include "gtest/gtest.h"
#include "src/coflow.h"
#include "src/traffic_generator.h"
#include "ximulator_test_base.h"

class TrafficGeneratorTest : public XimulatorTestBase {
 protected:
  virtual void TearDown() {}

  virtual void SetUp() {
    // disable db logging.
    traffic_generator_.reset(new TGTraceFB(nullptr/*db_logger*/));
  }

  void LoadAllCoflows(vector<Coflow *> &load_to_me) {
    vector<JobDesc *> new_jobs = traffic_generator_->ReadJobs();
    while (!new_jobs.empty()) {
      for (JobDesc *job : new_jobs) {
        load_to_me.push_back(job->m_coflow);
        // deleting a job would NOT delete the coflow pointer insides.
        delete job;
      }
      new_jobs = traffic_generator_->ReadJobs();
    }
  }

  std::unique_ptr<TGTraceFB> traffic_generator_;
};

TEST_F(TrafficGeneratorTest, TaskPlacementFromTrace) {

  vector<Coflow *> coflows;
  LoadAllCoflows(coflows);

  for (Coflow *coflow: coflows) {
    set<int> actual_mappers, actual_reducers;
    for (Flow *flow : *coflow->GetFlows()) {
      actual_mappers.insert(flow->GetSrc());
      actual_reducers.insert(flow->GetDest());
    }
    set<int> expected_mappers, expected_reducers;
    switch (coflow->GetJobId()) {
      case 2: //
        expected_mappers = set<int>({104, 132});
        expected_reducers = set<int>({140});
        break;
      case 54: //
        expected_mappers = set<int>({3, 66, 79, 121, 148});
        expected_reducers = set<int>({14, 100});
        break;
      case 108: //
        expected_mappers = set<int>({60, 89, 125, 126});
        expected_reducers = set<int>({14, 22, 31, 35, 45, 51, 54, 55, 60, 68,
                                      89, 99, 102, 115, 118});
        break;
      default:continue; // with next coflow
    }
    EXPECT_EQ(expected_mappers, actual_mappers);
    EXPECT_EQ(expected_reducers, actual_reducers);
  }
}

TEST_F(TrafficGeneratorTest, TaskPlacementWorstFit) {
  traffic_generator_.reset(new TGWorstFitPlacement(nullptr/*db_logger*/));

  vector<Coflow *> coflows;
  LoadAllCoflows(coflows);

  for (Coflow *coflow: coflows) {
    set<int> actual_mappers, actual_reducers;
    for (Flow *flow : *coflow->GetFlows()) {
      actual_mappers.insert(flow->GetSrc());
      actual_reducers.insert(flow->GetDest());
    }
    // All tasks starts on node 0,1,2... since no usage is incurred without job
    // submissions.
    set<int> expected_mappers, expected_reducers;
    switch (coflow->GetJobId()) {
      case 2: //
        expected_mappers = set<int>({0, 1});
        expected_reducers = set<int>({2});
        break;
      case 54: //
        expected_mappers = set<int>({0, 1, 2, 3, 4});
        expected_reducers = set<int>({5, 6});
        break;
      default:continue; // with next coflow
    }

    EXPECT_EQ(expected_mappers, actual_mappers);
    EXPECT_EQ(expected_reducers, actual_reducers);
  }
}

TEST_F(TrafficGeneratorTest, TaskPlacementNeat) {
  traffic_generator_.reset(new TGNeat(nullptr/*db_logger*/));

  vector<Coflow *> coflows;
  LoadAllCoflows(coflows);

  for (Coflow *coflow: coflows) {
    set<int> actual_mappers, actual_reducers;
    for (Flow *flow : *coflow->GetFlows()) {
      actual_mappers.insert(flow->GetSrc());
      actual_reducers.insert(flow->GetDest());
    }
    // Tasks usually start on node 0,1,2... as if they start as the only coflow
    // in the network, since no usage is incurred without job submissions.
    set<int> expected_mappers, expected_reducers;
    switch (coflow->GetJobId()) {
      case 2: //
        expected_mappers = set<int>({0, 2});
        expected_reducers = set<int>({1});
        break;
      case 54: //
        expected_mappers = set<int>({0, 2, 3, 5, 6});
        expected_reducers = set<int>({1, 4});
        break;
      default:continue; // with next coflow
    }

    EXPECT_EQ(expected_mappers, actual_mappers);
    EXPECT_EQ(expected_reducers, actual_reducers);
  }
}
