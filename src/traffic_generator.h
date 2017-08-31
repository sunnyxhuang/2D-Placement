#ifndef TRAFFIC_GENERATOR_H
#define TRAFFIC_GENERATOR_H

#include <assert.h>
#include <fstream>
#include <map>
#include <set>
#include <vector>

#include "global.h"
#include "db_logger.h"
#include "usage_monitor.h"

using namespace std;

class Flow;
class Coflow;
class JobDesc;
class Simulator;
class TrafficGenTimeLine;

class TrafficGen {
 public:
  TrafficGen();
  virtual ~TrafficGen();

  void InstallSimulator(Simulator *simPtr) { m_simPtr = simPtr; }
  /* called by simulator */
  virtual void NotifySimStart() = 0;
  virtual void TrafficGenAlarmPortal(double time) = 0;

  /* called by simulator */
  virtual void NotifyTrafficFinish(double alarmTime,
                                   vector<Coflow *> *cfpVp,
                                   vector<Flow *> *fpVp) = 0;
  virtual void NotifySimEnd() = 0;

 protected:
  virtual void PlaceTasks(int num_map, int num_red,
                          const map<pair<int, int>, long> &mr_flow_bytes,
                          const vector<int> &mapper_original_locations,
                          const vector<int> &reducer_original_locations,
                          vector<int> *mapper_locations,
                          vector<int> *reducer_locations) = 0;
  Simulator *m_simPtr;
  TrafficGenTimeLine *m_myTimeLine;

  DbLogger *db_logger_; // Not owned.
  std::unique_ptr<UsageMonitor> usage_monitor_;

  double m_currentTime;

  // internal auditing
  double m_totalCCT;
  double m_totalFCT;
  int m_totalCoflowNum;
  int m_total_accepted_coflow_num;
  int m_total_met_deadline_num;
  ifstream m_jobTraceFile;
  ofstream m_jobAuditFile;
  bool m_audit_file_title_line;

  void UpdateAlarm(); // update alarm on simulator

  friend class Simulator;
  friend class RememberForgetPlacementAnalyzer;
};

////////////////////////////////////////////////////
///////////// Code for FB Trace Replay   ///////////
////////////////////////////////////////////////////
class TGTraceFB : public TrafficGen {
 public:
  TGTraceFB(DbLogger *db_logger);
  virtual ~TGTraceFB();

  /* called by simulator */
  void NotifySimStart();
  void TrafficGenAlarmPortal(double time);

  /* called by simulator */
  virtual void NotifyTrafficFinish(double alarm_time,
                                   vector<Coflow *> *coflows_ptr,
                                   vector<Flow *> *flows_ptr);
  void NotifySimEnd();

 protected:
  vector<JobDesc *> m_runningJob; // allow Neat to access.
 private:
  vector<JobDesc *> m_finishJob;
  vector<JobDesc *> m_readyJob;

  virtual void DoSubmitJob();

  // Random seed bookeeping - one seed for each coflow.
  void InitSeedForCoflows(int seed_for_seed);
  int GetSeedForCoflow(int coflow_id);

  // Obtain reducer input bytes and the reducer assigned machines from
  // reducer_trace. reducer_trace is formated as
  // r1_location : r1_input; r2_location : r2_input; ...
  // input are rounded to the closest MB.
  // remember the locations and input bytes *in order* for each reducer.
  void GetReducerInfoFromString(int num_reducer_wanted,
                                const string &reducer_trace,
                                vector<int> *original_locations,
                                vector<long> *reducer_input_bytes);
  vector<int> GetMapperOriginalLocFromString(int num_mapper_wanted,
                                             const string &mapper_trace);
  // TODO: remove unused.
  map<pair<int, int>, long>
  GetFlowSizeWithExactSize(int numMap, int numRed,
                           const vector<long> &redInput);
  // TODO: remove unused.
  map<pair<int, int>, long>
  GetFlowSizeWithEqualSizeToSameReducer(int numMap, int numRed,
                                        const vector<long> &redInput);
  // flow sizes will be +/- 1MB * perturb_perc%.
  // perturb_perc is a percentage, i.e. when perturb_perc = 10, then
  //    flow sizes will be +/- 0.1 MB.
  // only allow flow >= 1MB.
  map<pair<int, int>, long>
  GetFlowSizeWithPerturb(int numMap, int numRed,
                         const vector<long> &redInput,
                         int perturb_perc, unsigned int rand_seed);

  // By default, place mappers and reducers on the orignal locations specified.
  virtual void PlaceTasks(int num_map, int num_red,
                          const map<pair<int, int>, long> &mr_flow_bytes,
                          const vector<int> &mapper_original_locations,
                          const vector<int> &reducer_original_locations,
                          vector<int> *mapper_locations,
                          vector<int> *reducer_locations) {
    assert(num_map == mapper_original_locations.size());
    assert(num_red == reducer_original_locations.size());
    *mapper_locations = mapper_original_locations;
    *reducer_locations = reducer_original_locations;
  }

 protected:
  virtual vector<JobDesc *> ReadJobs();

  virtual Coflow *CreateCoflowPtrFromString(double time, int coflow_id,
                                            int num_map, int num_red,
                                            string cfInfo,
                                            bool do_perturb, bool avg_size);
  void GetNodeReqTrafficMB(int num_map, int num_red,
                           const map<pair<int, int>, long> &mr_flow_bytes,
                           vector<double> *mapper_traffic_req_MB,
                           vector<double> *reducer_traffic_req_MB);

  void ScheduleToAddJobs(vector<JobDesc *> &jobs);
  void KickStartReadyJobsAndNotifyScheduler();
  map<Coflow *, JobDesc *> m_coflow2job;
  vector<unsigned int> m_seed_for_coflow;

  friend class TrafficGeneratorTest;
  friend class TrafficAnalyzerTest;
};

class JobDesc {
 public:
  JobDesc(int iid,
          double offTime,
          int numSrc,
          int numDst,
          int numFlow,
          Coflow *cfp) : m_id(iid),
                         m_offArrivalTime(offTime),
                         m_numSrc(numSrc),
                         m_numDst(numDst),
                         m_numFlow(numFlow),
                         m_coflow(cfp) {}
  ~JobDesc() {}

  int m_id; // task id
  double m_offArrivalTime; // offset arrival time
  int m_numSrc;
  int m_numDst;
  int m_numFlow;
  Coflow *m_coflow; // one coflow per task, not owned.
};


//
// Similar to TGTraceFB, but change mapper/reducer node assignment based
// on Worst Fit on resource, i.e. choosing the node whose load is the least.
//
class TGWorstFitPlacement : public TGTraceFB {
 public:
  TGWorstFitPlacement(DbLogger *db_logger) : TGTraceFB(db_logger) {}
  virtual ~TGWorstFitPlacement() {}
 protected:
  // ignore loc. pick the least loaded node. if there is a tie, pick node with
  // smaller index.
  virtual void PlaceTasks(int num_map, int num_red,
                          const map<pair<int, int>, long> &mr_flow_bytes,
                          const vector<int> &mapper_original_locations,
                          const vector<int> &reducer_original_locations,
                          vector<int> *mapper_locations,
                          vector<int> *reducer_locations) override;
  int GetNodeWithMinLoad(
      const vector<int> &candidates,
      set<int> *exclude_these_nodes,
      double additional_load,
      map<int, double> *expected_load);
};

class TG2DPlacement : public TGWorstFitPlacement {
 public:
  TG2DPlacement(DbLogger *db_logger) : TGWorstFitPlacement(db_logger) {}
 private:
  virtual void PlaceTasks(int num_map, int num_red,
                          const map<pair<int, int>, long> &mr_flow_bytes,
                          const vector<int> &mapper_original_locations,
                          const vector<int> &reducer_original_locations,
                          vector<int> *mapper_locations,
                          vector<int> *reducer_locations) override;
  static bool TrafficReqDesc(std::pair<int, double> const &a,
                             std::pair<int, double> const &b) {
    return a.second > b.second;
  }
};

class TGNeat : public TGWorstFitPlacement {
 public:
  TGNeat(DbLogger *db_logger, bool use_ascending_req = false)
      : TGWorstFitPlacement(db_logger), use_ascending_req_(use_ascending_req) {}
 private:
  // if true, place node according to ordered node req from small to large.
  bool use_ascending_req_;
  // ignore loc. pick the least loaded node. if there is a tie, pick node with
  // smaller index.
  virtual void PlaceTasks(int num_map, int num_red,
                          const map<pair<int, int>, long> &mr_flow_bytes,
                          const vector<int> &mapper_original_locations,
                          const vector<int> &reducer_original_locations,
                          vector<int> *mapper_locations,
                          vector<int> *reducer_locations) override;
 protected:
  // calculate the 'cost' of (base, factor) to place a flow onto a node, where
  // 1) _base_ is the load on node from coflows with *higher* priority
  // as specified with the alpha_cutoff (higher prioirty with lower alpha),
  // 2) _factor_ is the number of flows from coflow with *lower or the same*
  // priority.
  // Note alpha is in bits, and usually is the bottleneck of the coflow.
  void GetNeatAdjustedCostFuncMB(long alpha_cutoff,
                                 map<int, pair<double, int>> *send_cost_func,
                                 map<int, pair<double, int>> *recv_cost_func);
  map<int, pair<double, int>> GetNeatCostFuncFromQueue(
      long alpha_cutoff, bool send_direction,
      const map<int, vector<Coflow *>> &node_queue_map);

  // TODO:remove unused func.
  map<int, pair<double, int>> GetNeatCostFuncFromQueue(
      long alpha_cutoff, const map<int, vector<Flow *>> &node_queue_map);
  map<int, pair<double, int>> GetNeatAdjustedCostFuncMB(long alpha_cutoff) {
    return GetNeatCostFuncFromQueue(
        alpha_cutoff, usage_monitor_->GetIOFlowQueues());
  }

  // Return node to place based on neat cost function, update cost_func after
  // placement.
  int GetNodeWithNeat(const map<int, vector<Flow *>> &node_queue_map,
                      double add_load_MB,
                      set<int> *exclude_these_nodes,
                      map<int, pair<double, int>> *cost_func_MB);
};

///////////////////////////////////////////////////////////////////////////////
///////////// Code for quick analyze of coflows
///////////////////////////////////////////////////////////////////////////////

class AnalyzeBase : public TGTraceFB {
 public:
  AnalyzeBase(DbLogger *db_logger) : TGTraceFB(db_logger) {
    analyze_title_line_ = false;
  }
 private:
  bool analyze_title_line_;
 protected:
  // load and analyze coflow. log coflow characteristic merics into db if
  // db_logger is valid.
  void LoadAllCoflows(vector<Coflow *> *load_to_me);
  void CleanUpCoflows(vector<Coflow *> *clean_me);

  friend class UsageMonitorTest;
};

#endif /* TRAFFIC_GENERATOR_H */