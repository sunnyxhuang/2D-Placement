//
// Created by Xin Sunny Huang on 10/22/16.
//

#include "gtest/gtest.h"
#include "src/events.h"
#include "ximulator_test_base.h"

class XimulatorTest : public XimulatorTestBase {
 protected:
  virtual void TearDown() {
  }

  virtual void SetUp() {
    ximulator_.reset(new Simulator());
  }
  std::unique_ptr<Simulator> ximulator_;
};

TEST_F(XimulatorTest, VarysOnFB) {
  ximulator_->InstallScheduler("varysImpl");
  ximulator_->InstallTrafficGen("fb", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(), 2.640789, 1e-6);
}

TEST_F(XimulatorTest, AaloOnFB) {
  ximulator_->InstallScheduler("aaloImpl");
  ximulator_->InstallTrafficGen("fb", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(), 3.004975, 1e-6);
}


TEST_F(XimulatorTest, VarysOn2DPlacement) {
  ximulator_->InstallScheduler("varysImpl");
  ximulator_->InstallTrafficGen("2DPlace", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(), 4.425327, 1e-6);
}


TEST_F(XimulatorTest, AaloOn2DPlacement) {
  ximulator_->InstallScheduler("aaloImpl");
  ximulator_->InstallTrafficGen("2DPlace", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(), 5.771111, 1e-6);
}

TEST_F(XimulatorTest, VarysOnNeat) {
  ximulator_->InstallScheduler("varysImpl");
  ximulator_->InstallTrafficGen("Neat", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(), 5.444291, 1e-6);
}


TEST_F(XimulatorTest, AaloOnNeat) {
  ximulator_->InstallScheduler("aaloImpl");
  ximulator_->InstallTrafficGen("Neat", &db_logger_);
  ximulator_->Run();
  EXPECT_NEAR(ximulator_->GetTotalCCT(), 21.846574, 1e-6);
}