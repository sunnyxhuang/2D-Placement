#ifndef COFLOW_H
#define COFLOW_H

#include <vector>
#include "util.h"

#include <map>
#include <iostream>

using namespace std;

class Coflow;

class Flow {
 public:
  Flow(double startTime, int src, int dest, long sizeInByte);
  ~Flow();
  int GetFlowId() { return m_flowId; }

  double GetStartTime() { return m_startTime; }
  void SetEndTime(double endtime) { m_endTime = endtime; }
  double GetEndTime() { return m_endTime; }

  void SetParentCoflow(Coflow *p) { parent_coflow_ = p; }
  Coflow *GetParentCoflow() { return parent_coflow_; }

  int GetSrc() { return m_src; }
  int GetDest() { return m_dest; }
  long GetSizeInBit() { return m_sizeInBit; }
  long GetBitsLeft() { return m_bitsLeft; }
  long GetElecRate(void) { return m_elecBps; }
  long GetOptcRate(void) { return m_optcBps; }
  void SetRate(long elecBps, long optcBps);
  void SetThruOptic(bool thruOptc) { m_thruOptic = thruOptc; }
  bool isThruOptic() { return m_thruOptic; }
  bool isRawFlow();
  long Transmit(double startTime, double endTime);// return bitsLeft
  long TxSalvage();// clear flow if smaller than threshold.
  long TxLocal();  // clear flow if local rack; return bitsLeft otherwise
  string toString();
 private:
  static int s_flowIdTracker;
  int m_flowId;
  int m_src;
  int m_dest;
  bool m_thruOptic;
  long m_sizeInBit;
  long m_bitsLeft;
  double m_startTime;
  double m_endTime;
  long m_elecBps;
  long m_optcBps;
  Coflow *parent_coflow_; // not owned.
};

class CompTimeBreakdown {
 public:
  CompTimeBreakdown() {
    m_time_shuffle_random = 0.0;
    m_time_sort = 0.0;
    m_time_reserve = 0.0;
    m_time_stuff = 0.0;
    m_time_slice = 0.0;
  }
  double GetSunflowTotalTime() {
    return m_time_shuffle_random
        + m_time_sort
        + m_time_reserve;
  }
  double GetSolsticeTotalTime() {
    return m_time_stuff + m_time_slice;
  }
  double GetVectorAvgTime() {
    // for ansop
    if (m_time_vector.empty()) return 0.0;
    double sum = 0.0;
    for (vector<double>::const_iterator
             t = m_time_vector.begin();
         t != m_time_vector.end();
         t++) {
      sum += *t;
    }
    return sum / m_time_vector.size(); // avg
  }
  double GetVectorMaxTime() {
    // for ansop
    double max = -1.0;
    for (vector<double>::const_iterator
             t = m_time_vector.begin();
         t != m_time_vector.end();
         t++) {
      if (max < *t || max < 0) {
        max = *t;
      }
    }
    return max;
  }
  double GetVectorMinTime() {
    // for ansop
    double min = -1.0;
    for (vector<double>::const_iterator
             t = m_time_vector.begin();
         t != m_time_vector.end();
         t++) {
      if (min > *t || min < 0) {
        min = *t;
      }
    }
    return min;
  }
  // for sunflow
  double m_time_shuffle_random;
  double m_time_sort;
  double m_time_reserve;
  // for conext
  double m_time_stuff;
  double m_time_slice;
  // for ansop
  vector<double> m_time_vector;
};

class Coflow {
 public:
  Coflow(double startTime, int totalFlows);
  ~Coflow();
  int GetCoflowId() { return m_coflowId; }
  const int GetJobId() { return m_job_id; }
  void SetJobId(int job_id) { m_job_id = job_id; }
  double GetEndTime() { return m_endTime; }
  void SetEndTime(double endTime) { m_endTime = endTime; }
  void SetPlacement(const vector<int> &mapper_locations,
                    const vector<int> &reducer_locations) {
    mapper_locations_ = mapper_locations;
    reducer_locations_ = reducer_locations;
  }
  const vector<int> &GetMapperLocations() { return mapper_locations_; }
  const vector<int> &GetReducerLocations() { return reducer_locations_; }
  int GetNumMap() { return mapper_locations_.size(); }
  int GetNumRed() { return reducer_locations_.size(); }
  void SetMRFlowBytes(const map<pair<int, int>, long> &mr_flow_bytes) {
    mr_flow_bytes_ = mr_flow_bytes;
  }
  const map<pair<int, int>, long> &GetMRFlowBytes() { return mr_flow_bytes_; }

  void SetMapReduceLoadMB(const vector<int> &mapper_locations,
                          const vector<int> &reducer_locations,
                          const vector<double> &mapper_load_MB,
                          const vector<double> &reducer_load_MB) {
    if (mapper_locations.size() != mapper_load_MB.size() ||
        reducer_locations.size() != reducer_load_MB.size()) {
      cerr << "mapper/reducer locations and load_MB vectors size mismatched!\n";
      exit(-1);
    }
    for (int idx = 0; idx < mapper_locations.size(); idx++) {
      mapper_load_MB_[mapper_locations[idx]] = mapper_load_MB[idx];
    }
    for (int idx = 0; idx < reducer_locations.size(); idx++) {
      reducer_load_MB_[reducer_locations[idx]] = reducer_load_MB[idx];
    }
  }
  double GetSendLoadMB(int node) { return mapper_load_MB_[node]; }
  double GetRecvLoadMB(int node) { return reducer_load_MB_[node]; }
  double GetMaxMapRedLoadGb() {
    return max(MaxMap(mapper_load_MB_), MaxMap(reducer_load_MB_)) * 8 / 1e3;
  }

  void AddFlow(Flow *f);

  long GetMaxPortLoadInBits();
  double GetMaxPortLoadInSec();
  double GetMaxPortLoadInSeconds(); // elec lowerbound
  double GetMaxOptimalWorkSpanInSeconds();
  long GetLoadOnMaxOptimalWorkSpanInBits(); // optc lowerbound
  void GetPortOnMaxOptimalWorkSpan(int &src, int &dst);
  long GetLoadOnPortInBits(int src, int dst);
  double GetOptimalWorkSpanOnPortInSeconds(int src, int dst);
  double GetSizeInByte() { return m_coflow_size_in_bytes; }

  void AddTxBit(long bit_sent) {
    m_coflow_sent_bytes += (double) bit_sent / 8.0;
  };
  double GetSentByte() { return m_coflow_sent_bytes; };

  void SetDeadlineSec(double d) { m_deadline_duration = d; }
  double GetDeadlineSec() { return m_deadline_duration; }

  bool IsRejected() { return m_is_rejected; }
  void SetRejected() { m_is_rejected = true; }

  void SetCompTime(CompTimeBreakdown comptime) { m_comp_time = comptime; }
  void AddTimeToVector(double one_slot_computation_time) {
    m_comp_time.m_time_vector.push_back(one_slot_computation_time);
  }
  CompTimeBreakdown GetCompTime() { return m_comp_time; }

  void SetStaticAlpha(long alpha) { m_static_alpha = alpha; }
  long GetStaticAlpha() { return m_static_alpha; }

  long CalcAlpha();
  long GetAlpha() { return m_alpha; }

  double CalcAlphaOnline(map<int, long> &sBpsFree,
                         map<int, long> &rBpsFree,
                         long LINK_RATE_BPS);
  double GetAlphaOnline() { return m_online_alpha; }

  double GetStartTime() { return m_startTime; }
  bool IsComplete() { return m_nFlowsCompleted >= m_nTotalFlows; }
  bool IsFlowsAddedComplete() { return m_nFlowsCompleted >= m_nFlows; }

  vector<Flow *> *GetFlows(void) { return m_flowVector; }

  bool NumFlowFinishInc();
  bool isRawCoflow();

  void Print();
  string toString();
 private:
  vector<Flow *> *m_flowVector;
  static int s_coflowIdTracker;

  int m_job_id;

  map<int, double> mapper_load_MB_;
  map<int, double> reducer_load_MB_;
  vector<int> mapper_locations_;
  vector<int> reducer_locations_;
  // map from (mapper_idx, reducer_idx) to the size of flows between them.
  map<pair<int, int>, long> mr_flow_bytes_;

  int m_coflowId;
  int m_nFlows;
  int m_nFlowsCompleted;
  int m_nTotalFlows;

  // Expected CCT based on packet switching.
  long m_static_alpha;
  long m_alpha;
  double m_online_alpha;

  double m_startTime; /*earliest start time of flow contained*/
  double m_endTime;
  // deadline duration after arrival time.
  double m_deadline_duration;
  bool m_is_rejected;

  // coflow profile.
  map<int, long> m_src_bits;
  map<int, long> m_dst_bits;

  map<int, double> m_src_time;
  map<int, double> m_dst_time;

  double m_coflow_size_in_bytes;

  // aalo
  double m_coflow_sent_bytes;

  CompTimeBreakdown m_comp_time;

  /* data */
};

bool coflowCompAlpha(Coflow *l, Coflow *r);
bool coflowCompArrival(Coflow *l, Coflow *r);
#endif /*COFLOW_H*/
