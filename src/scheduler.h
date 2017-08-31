#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <vector>
#include <fstream>
#include <queue>
#include <set>
#include <map>

#include "usage_monitor.h"

using namespace std;

class Flow;
class Coflow;
class CompTimeBreakdown;
class Simulator;
class SchedulerTimeLine;

class Scheduler {
 public:
  Scheduler();
  virtual ~Scheduler();
  void InstallSimulator(Simulator *simPtr) { m_simPtr = simPtr; }
  virtual void SchedulerAlarmPortal(double) = 0;
  void NotifySimEnd();

  virtual void NotifyAddCoflows(double, vector<Coflow *> *);
  virtual void NotifyAddFlows(double);

 protected:
  Simulator *m_simPtr;
  double m_currentTime;
  SchedulerTimeLine *m_myTimeLine;
  vector<Coflow *> m_coflowPtrVector;

  map<int, long> m_nextElecRate;          /* maps flow ID to next elec rate */
  map<int, long> m_nextOptcRate;          /* maps flow ID to next optc rate */

  map<int, long> m_sBpsFree_before_workConserv;
  map<int, long> m_rBpsFree_before_workConserv;

  // map from src/dst id -> tx in bits
  map<int, long> m_validate_last_tx_src_bits;
  map<int, long> m_validate_last_tx_dst_bits;

  // returns true if has flow finish during transfer.
  virtual bool Transmit(double startTime,
                        double endTime,
                        bool basic,
                        bool local,
                        bool salvage);
  virtual void ScheduleToNotifyTrafficFinish(double end_time,
                                             vector<Coflow *> &coflows_done,
                                             vector<Flow *> &flows_done);
  virtual void CoflowFinishCallBack(double finishtime) = 0;
  virtual void FlowFinishCallBack(double finishTime) = 0;

  // sort coflows based on different policiess.
  void CalAlphaAndSortCoflowsInPlace(vector<Coflow *> &coflows);

  bool ValidateLastTxMeetConstraints(long port_bound_bits);

  void SetFlowRate();

  void UpdateAlarm();
  void UpdateRescheduleEvent(double reScheduleTime);
  void UpdateFlowFinishEvent(double baseTime);

  double SecureFinishTime(long bits, long rate);
  double CalcTime2FirstFlowEnd();
  void Print(void);
 private:
  // mark down circuit activities in schedulerOptc.
  // otherwise no-op.
  virtual void CircuitAuditIfNeeded(double beginTime,
                                    double endTime) {};
  virtual void WriteCircuitAuditIfNeeded(double endTime,
                                         vector<Coflow *> &coflows_done) {};
};

class SchedulerVarys : public Scheduler {
 public:
  SchedulerVarys();
  virtual ~SchedulerVarys();
  void SchedulerAlarmPortal(double currentTime);
 protected:
  // override by Varys-Deadline.
  virtual void CoflowArrive();

 private:
  virtual void Schedule(void) = 0;
  // override by Aalo.
  virtual void AddCoflows(vector<Coflow *> *cfVecPtr);

  void ApplyNewSchedule(void);
  void AddFlows();
  void FlowArrive();
  void FlowFinishCallBack(double finishTime);
  void CoflowFinishCallBack(double finishTime);

};

class SchedulerAaloImpl : public SchedulerVarys {
 public:
  SchedulerAaloImpl();
  virtual ~SchedulerAaloImpl() {}
 private:
  virtual void Schedule(void);

  virtual void AddCoflows(vector<Coflow *> *cfVecPtr);

  void RateControlAaloImpl(vector<Coflow *> &coflows,
                           vector<vector<int>> &coflow_id_queues,
                           map<int, long> &rates,
                           long LINK_RATE);
  void UpdateCoflowQueue(vector<Coflow *> &coflows,
                         vector<vector<int>> &last_coflow_id_queues,
                         map<int, Coflow *> &coflow_id_ptr_map);
  // used to maintain the stability of coflow queues.
  vector<vector<int>> m_coflow_jid_queues;
};

class SchedulerVarysImpl : public SchedulerVarys {
 public:
  SchedulerVarysImpl() : SchedulerVarys() {}
  virtual ~SchedulerVarysImpl() {}
 private:
  virtual void Schedule(void);

  // Varys implemented in Github!
  // rate control based on selfish coflow
  // and the work conservation for each coflow's flow.
  // store flow rates in rates.
  void RateControlVarysImpl(vector<Coflow *> &coflows,
                            map<int, long> &rates,
                            long LINK_RATE);

 protected:
  // as used by the deadline-mode varysImpl scheduler.
  // routine used RateControlVarysImpl.
  // work conservation in the Github implementation.
  void RateControlWorkConservationImpl(vector<Coflow *> &coflows,
                                       map<int, long> &rates,
                                       map<int, long> &sBpsFree,
                                       map<int, long> &rBpsFree,
                                       long LINK_RATE_BPS);
};


#endif /*SCHEDULER_H*/
