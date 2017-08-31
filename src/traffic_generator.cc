#include <iomanip>
#include <algorithm> // needed for find in NotifyTrafficFinish()
//#include <cfloat>
#include <ctgmath>
#include <numeric> //dbg
#include <random>
//#include <sstream>

#include "coflow.h"
#include "events.h"
#include "traffic_generator.h"
#include "util.h"

using namespace std;

TrafficGen::TrafficGen() {
  m_currentTime = 0;
  m_simPtr = NULL;
  m_myTimeLine = new TrafficGenTimeLine;

  m_totalCCT = 0;
  m_totalFCT = 0;

  m_totalCoflowNum = 0;
  m_total_accepted_coflow_num = 0;
  m_total_met_deadline_num = 0;
  m_audit_file_title_line = false;

  db_logger_ = nullptr;
  usage_monitor_ = nullptr;
}

TrafficGen::~TrafficGen() {

  delete m_myTimeLine;

  // Trigger flush all usage samples into db_logger inside usage_monitor.
  if (usage_monitor_) {
    usage_monitor_.reset(nullptr);
  }

  cout << "Done" << " ";
  cout << m_total_met_deadline_num
       << "/" << m_total_accepted_coflow_num
       << "/" << m_totalCoflowNum << " ";
  cout << "" << m_totalCCT << " ";
  cout << "" << m_totalFCT << " ";
  cout << endl;

  if (m_jobTraceFile.is_open() && m_jobAuditFile.is_open()) {
    m_jobTraceFile.close();
    //trace files
    m_jobAuditFile << "Done" << " ";
    m_jobAuditFile << m_total_met_deadline_num
                   << "/" << m_total_accepted_coflow_num
                   << "/" << m_totalCoflowNum << " ";
    m_jobAuditFile << "" << m_totalCCT << " ";
    m_jobAuditFile << "" << m_totalFCT << " ";

    m_jobAuditFile << endl;
    m_jobAuditFile.close();
  }
}

void
TrafficGen::UpdateAlarm() {
  if (!m_myTimeLine->isEmpty()) {
    Event *nextEvent = m_myTimeLine->PeekNext();
    double nextTime = nextEvent->GetEventTime();
    Event *ep = new Event(ALARM_TRAFFIC, nextTime);
    m_simPtr->UpdateTrafficAlarm(ep);
  }
}

////////////////////////////////////////////////////
///////////// Code for FB Trace Replay   ///////////
////////////////////////////////////////////////////

TGTraceFB::TGTraceFB(DbLogger *db_logger) {

  db_logger_ = db_logger;
  usage_monitor_.reset(new UsageMonitor(db_logger));

  m_readyJob = vector<JobDesc *>();
  m_runningJob = vector<JobDesc *>();

  m_coflow2job = map<Coflow *, JobDesc *>();

  m_jobTraceFile.open(TRAFFIC_TRACE_FILE_NAME);
  if (!m_jobTraceFile.is_open()) {
    cout << "Error: unable to open file "
         << TRAFFIC_TRACE_FILE_NAME << endl;
    cout << "Now terminate the program" << endl;
    exit(-1);
  }
  // trace files
  m_jobAuditFile.open(TRAFFIC_AUDIT_FILE_NAME);
  if (!m_jobAuditFile.is_open()) {
    cout << "Error: unable to open file "
         << TRAFFIC_AUDIT_FILE_NAME << endl;
    cout << "Now terminate the program" << endl;
    exit(-1);
  }

  int seed_for_perturb_seed = 13;
  InitSeedForCoflows(seed_for_perturb_seed);

}

TGTraceFB::~TGTraceFB() {
  if (!m_runningJob.empty()) {
    //error
    cout << "[TGTraceFB::~TGTraceFB()] Error:"
         << "has unfinished traffic at the end of simulation!"
         << endl;
  }
}

/* called by simulator */
void
TGTraceFB::NotifySimStart() {
  vector<JobDesc *> jobs2add = ReadJobs();
  ScheduleToAddJobs(jobs2add);
  UpdateAlarm();
}

void
TGTraceFB::TrafficGenAlarmPortal(double alarmTime) {

  while (!m_myTimeLine->isEmpty()) {
    Event *currentEvent = m_myTimeLine->PeekNext();
    double currentEventTime = currentEvent->GetEventTime();

    if (currentEventTime > alarmTime) {
      // break if we have over run the clock
      break;
    }
    // it seems everything is ok
    m_currentTime = alarmTime;

    cout << fixed << setw(FLOAT_TIME_WIDTH) << currentEventTime << "s "
         << "[TGTraceFB::TrafficGenAlarmPortal] "
         << " working on event type " << currentEvent->GetEventType() << endl;

    //currentEvent->CallBack();
    switch (currentEvent->GetEventType()) {
      case SUB_JOB:DoSubmitJob();
        break;
      default:break;
    }

    Event *e2p = m_myTimeLine->PopNext();
    if (e2p != currentEvent) {
      cout << "[TGTraceFB::SchedulerAlarmPortal] error: "
           << " e2p != currentEvent " << endl;
    }
    delete currentEvent;
  }

  UpdateAlarm();
}

// Called by simulator. The Scheduler class adds an event in the simulator.
// The simulator event invokes this function.
void
TGTraceFB::NotifyTrafficFinish(double alarm_time,
                               vector<Coflow *> *coflows_ptr,
                               vector<Flow *> *flows_ptr) {

  for (Flow *flow : *flows_ptr) {
    if (flow->GetEndTime() != INVALID_TIME
        && flow->GetEndTime() >= flow->GetStartTime()) {
      // This flow is properly done.
      // A valid start/end time => count FCT
      m_totalFCT += (flow->GetEndTime() - flow->GetStartTime());
      // release resource usage if needed.
      if (usage_monitor_ && flow->GetParentCoflow()) {
        usage_monitor_->Unregister(alarm_time, flow->GetParentCoflow(), flow);
      }
      if (db_logger_) {
        db_logger_->WriteOnFlowFinish(alarm_time, flow);
      }
    } else {
      // sth is wrong.
      cout << "Error while calculating flow end time: invalid start/end time\n";
      exit(-1);
    }
  }

  for (Coflow *coflow : *coflows_ptr) {
    // count coflow num no matter what.
    m_totalCoflowNum++;

    if (coflow->IsRejected()) {
      // only count completed coflow.
      // do not consider rejected coflow.
      continue;
    }
    m_total_accepted_coflow_num++;

    double cct = INVALID_TIME;

    if (coflow->GetEndTime() != INVALID_TIME
        && coflow->GetEndTime() >= coflow->GetStartTime()) {
      // This coflow is properly finished.
      // a valid end time => count CCT
      cct = coflow->GetEndTime() - coflow->GetStartTime();
      // release resource usage if needed.
      if (usage_monitor_) {
        usage_monitor_->Unregister(alarm_time, coflow);
      }
      if (db_logger_) {
        db_logger_->WriteOnCoflowFinish(alarm_time, coflow);
      }
    } else {
      // sth is wrong.
      cout << " error while calculating coflow end time: invalid start/end time"
           << endl;
      exit(-1);
    }

    if (coflow->GetDeadlineSec() <= 0) {
      // no deadline.
      m_totalCCT += cct;
    } else {
      // consider deadline.
      if (cct <= (coflow->GetDeadlineSec() + DEADLINE_ERROR_TOLERANCE)) {
        // met deadline.
        // only count CCT on accepted coflows.
        m_total_met_deadline_num++;
        m_totalCCT += cct;
      }
    }
  }

  // remove the job and coflows
  for (Coflow *coflow : *coflows_ptr) {
    map<Coflow *, JobDesc *>::iterator jobmapIt = m_coflow2job.find(coflow);
    if (jobmapIt == m_coflow2job.end()) {
      cout << "error: can't locate the job from the coflow ptr" << endl;
      cout << "return without clearing up the dead job" << endl;
      return;
    }
    JobDesc *jp2rm = jobmapIt->second;

    delete coflow; // Also deletes the flow pointers under this coflow.
    delete jp2rm;

    m_coflow2job.erase(jobmapIt);

    vector<JobDesc *>::iterator runJIt = find(m_runningJob.begin(),
                                              m_runningJob.end(),
                                              jp2rm);

    if (runJIt == m_runningJob.end()) {
      cout << "Error: the job to delete is not found in the running jobs!\n";
    } else {
      m_runningJob.erase(runJIt);
      m_finishJob.push_back(jp2rm);
    }
  }
}

void
TGTraceFB::NotifySimEnd() {

}

void
TGTraceFB::DoSubmitJob() {

  KickStartReadyJobsAndNotifyScheduler();

  // add next job
  vector<JobDesc *> nextjobs2add = ReadJobs();
  ScheduleToAddJobs(nextjobs2add);
}

// We assume one coflow per event, per job.
void
TGTraceFB::KickStartReadyJobsAndNotifyScheduler() {

  EventSubmitJobDesc *ep = (EventSubmitJobDesc *) m_myTimeLine->PeekNext();
  if (ep->GetEventType() != SUB_JOB) {
    cout << "[TGTraceFB::DoSubmitJob] error: "
         << " the event type is not SUB_JOB!" << endl;
    return;
  }

  vector<JobDesc *>::iterator jIt = find(m_readyJob.begin(),
                                         m_readyJob.end(),
                                         ep->m_jobPtr);

  if (jIt == m_readyJob.end()) {
    cout << "error: the job to submit is not in the "
         << " ready job set!" << endl;
  } else {
    m_readyJob.erase(jIt);
    m_runningJob.push_back(ep->m_jobPtr);
  }

  // dump traffic into network
  vector<Coflow *> *msgCfVp = new vector<Coflow *>();
  msgCfVp->push_back(ep->m_jobPtr->m_coflow);
  MsgEventAddCoflows *msgEp = new MsgEventAddCoflows(m_currentTime, msgCfVp);
  m_simPtr->AddEvent(msgEp);

  // start accounting resource usage for this coflow
  if (usage_monitor_) {
    usage_monitor_->Register(m_currentTime, ep->m_jobPtr->m_coflow);
  }
  if (db_logger_) {
    db_logger_->WriteCoflowFeatures(ep->m_jobPtr->m_coflow);
  }
}

void
TGTraceFB::ScheduleToAddJobs(vector<JobDesc *> &jobs_to_add) {
  if (jobs_to_add.empty()) return;
  for (vector<JobDesc *>::iterator jobIt = jobs_to_add.begin();
       jobIt != jobs_to_add.end(); jobIt++) {
    EventSubmitJobDesc *ep =
        new EventSubmitJobDesc((*jobIt)->m_offArrivalTime, *jobIt);
    m_myTimeLine->AddEvent(ep);
    m_readyJob.push_back(*jobIt);
  }
}

// read next job(s)
// the last job trace line
// should end with return
vector<JobDesc *>
TGTraceFB::ReadJobs() {

  vector<JobDesc *> result;

  string jobLine = "";

  long firstJobTime = -1;

  while (!m_jobTraceFile.eof()) {
    getline(m_jobTraceFile, jobLine);

    if (jobLine.size() <= 0) {
      //cout << "no more jobs are available!" << endl;
      return result;
    }

    vector<string> subFields;
    long numFields = split(jobLine, subFields, '\t');

    if (numFields != 5) {
      cout << "[TGTraceFB::ReadJobs] number of fields illegal!"
           << "Return with job list." << endl;
      return result;
    }

    long jobOffArrivalTimel = stol(subFields[1]);

    if (firstJobTime < 0) {
      firstJobTime = jobOffArrivalTimel;
    }

    if (jobOffArrivalTimel > firstJobTime) {
      // this job should not be read
      //seek back file seeker and return
      m_jobTraceFile.seekg(-(jobLine.length() + 1), m_jobTraceFile.cur);
      return result;
    }

    int jobid = stoi(subFields[0]);
    double jobOffArrivalTime = stod(subFields[1]) / 1000.0;
    int map = stoi(subFields[2]);
    int red = stoi(subFields[3]);

    // perturb if needed.
    // when perturb = false,
    //  if EQUAL_FLOW_TO_SAME_REDUCER = true, all flows to the same reducer
    //     will be the of the same size.
    Coflow *cfp = CreateCoflowPtrFromString(jobOffArrivalTime, jobid,
                                            map, red, subFields[4],
                                            ENABLE_PERTURB_IN_PLAY,
                                            EQUAL_FLOW_TO_SAME_REDUCER);
    cfp->SetJobId(jobid);
    // cfp->SetNumMap(map);
    // cfp->SetNumRed(red);

    int num_flow = 0;
    if (cfp) {
      num_flow = (int) cfp->GetFlows()->size();
    } else {
      cout << "Error: cfp NULL upon create!" << endl;
    }
    JobDesc *newJobPtr = new JobDesc(jobid,
                                     jobOffArrivalTime,
                                     map, red, num_flow, cfp);
    // add entry into the map
    m_coflow2job.insert(pair<Coflow *, JobDesc *>(cfp, newJobPtr));
    result.push_back(newJobPtr);
  }
  return result;
}

class ResourceRequestGenerator {
 public:
  ResourceRequestGenerator(const double rv_values[],
                           const int rv_prob[],
                           int sample_num, int seed) :
      rv_values_(std::vector<double>(rv_values, rv_values + sample_num)),
      rv_prob_(std::vector<int>(rv_prob, rv_prob + sample_num)),
      distribution_(std::discrete_distribution<int>(
          rv_prob_.begin(), rv_prob_.end())),
      generator_(seed) {}
  double Rand() {
    return rv_values_[distribution_(generator_)];
  }
 private:
  const std::vector<double> rv_values_;
  const std::vector<int> rv_prob_;
  std::mt19937 generator_;
  std::discrete_distribution<int> distribution_;
};

Coflow *
TGTraceFB::CreateCoflowPtrFromString(double time, int coflow_id,
                                     int num_map, int num_red,
                                     string cfInfo,
                                     bool do_perturb, bool avg_size) {
  //  cout << "[TGTraceFB::CreateCoflowPtrFromString] "
  //       << "Creating coflow #"<< coflow_id << endl;

  vector<string> subFields;
  if (split(cfInfo, subFields, '#') != 2) {
    cout << __func__ << ": number of fields illegal!"
         << "Return with NULL coflow ptr." << endl;
    return NULL;
  }

  // Obtain traffic requirements.
  // map from (mapper_idx, reducer_idx) to flow size in bytes. Mappers and
  // reducers are virtually indexed within this coflow. The actual placement of
  // the mapper and reducer tasks are to be determined.
  vector<int> mapper_original_locations, reducer_original_locations;
  mapper_original_locations = GetMapperOriginalLocFromString(num_map,
                                                             subFields[0]);
  vector<long> reducer_input_bytes;
  GetReducerInfoFromString(num_red, subFields[1],
                           &reducer_original_locations,
                           &reducer_input_bytes);

  map<pair<int, int>, long> mr_flow_bytes;
  if (!do_perturb) {
    if (avg_size) {
      mr_flow_bytes = GetFlowSizeWithEqualSizeToSameReducer(
          num_map, num_red, reducer_input_bytes);
    } else {
      mr_flow_bytes = GetFlowSizeWithExactSize(
          num_map, num_red, reducer_input_bytes);
    }
  } else {
    // let us allow some random perturbation.
    unsigned int rand_seed = GetSeedForCoflow(coflow_id);
    mr_flow_bytes = GetFlowSizeWithPerturb(
        num_map, num_red, reducer_input_bytes,
        5/* hard code of +/-5% */, rand_seed);
  }
  if (mr_flow_bytes.size() != num_map * num_red) {
    cout << "Error: The number of flows does not match. Exit with error.\n";
    exit(-1);
  }
  if (TRAFFIC_SIZE_INFLATE != 1.0) {
    for (auto &pair: mr_flow_bytes) {
      pair.second *= TRAFFIC_SIZE_INFLATE;
      if (pair.second < 1e6) {
        pair.second = 1e6; // all flows >= 1MB;
      }
    }
  }
  // Use current usage and the traffic/resource requirements to place mapper and
  // reducer tasks.
  // For small coflows whose (num_map + num_red < NUM_RACKS), we further requires
  // all mappers and reducers are on different nodes.
  // For the rest large coflows, we assume all mappers (reducers) are on
  // different nodes. However, mapper and reducer may share the same node, i.e.
  // flows may have the same src and dst.
  // 1/2 place mapper tasks
  vector<int> mapper_locations, reducer_locations;
  PlaceTasks(num_map, num_red, mr_flow_bytes,
             mapper_original_locations, reducer_original_locations,
             &mapper_locations, &reducer_locations);

  if (mapper_locations.size() != num_map
      || reducer_locations.size() != num_red) {
    cout << "Error while assigning machine locations. "
         << "Size of placemnet does not match. Exit with error.\n";
    exit(-1);
  }

  // Create flows by marrying the placement decisions with traffic requirements.
  vector<Flow *> flows;
  for (const auto &kv_pair : mr_flow_bytes) {
    int mapper_idx = kv_pair.first.first;
    int reducer_idx = kv_pair.first.second;
    long flow_bytes = kv_pair.second;
    flows.push_back(new Flow(time,
                             mapper_locations[mapper_idx],
                             reducer_locations[reducer_idx],
                             flow_bytes));
  }
  Coflow *coflow = new Coflow(time, flows.size());
  for (Flow *flow : flows) {
    coflow->AddFlow(flow);
    flow->SetParentCoflow(coflow);
  }
  coflow->SetPlacement(mapper_locations, reducer_locations);
  coflow->SetMRFlowBytes(mr_flow_bytes);
  // initialize the static alpha upon creation.
  coflow->SetStaticAlpha(coflow->CalcAlpha());

  // Calculate max traffic load requested on individual mapper or reducer node.
  // This metric will NOT change with different placement.
  vector<double> mapper_traffic_req_MB, reducer_traffic_req_MB;
  GetNodeReqTrafficMB(num_map, num_red, mr_flow_bytes,
                      &mapper_traffic_req_MB, &reducer_traffic_req_MB);
  coflow->SetMapReduceLoadMB(mapper_locations, reducer_locations,
                             mapper_traffic_req_MB, reducer_traffic_req_MB);

  // generate a deadline here if needed!
  if (DEADLINE_MODE) {
    double lb_optc = coflow->GetMaxOptimalWorkSpanInSeconds();
    double lb_elec
        = ((double) coflow->GetLoadOnMaxOptimalWorkSpanInBits()) / ELEC_BPS;
    unsigned int rand_seed = GetSeedForCoflow(coflow_id);
    std::mt19937 mt_rand(rand_seed); // srand(rand_seed);
    // currently we assume the inflation x = 1;
    double deadline
        = lb_optc + lb_optc * (((double) (mt_rand() % 100)) / 100.0);
    coflow->SetDeadlineSec(deadline);
    if (DEBUG_LEVEL >= 10) {
      cout << " lb_elec " << lb_elec << " lb_optc " << lb_optc
           << " deadline " << deadline << endl;
    }
  }

  return coflow;
}

vector<int>
TGTraceFB::GetMapperOriginalLocFromString(int num_mapper_wanted,
                                          const string &mapper_trace) {
  vector<string> locations;
  long num_fields = split(mapper_trace, locations, ',');
  if (num_fields != num_mapper_wanted) {
    cout << "ERROR: num_fields != num_mapper_wanted. "
         << "Return empty assignments." << endl;
    return vector<int>();
  }
  vector<int> result;
  for (const string &loc : locations) {
    result.push_back(std::stoi(loc));
  }
  return result;
}

void TGTraceFB::GetReducerInfoFromString(int num_reducer_wanted,
                                         const string &reducer_trace,
                                         vector<int> *original_locations,
                                         vector<long> *reducer_input_bytes) {
  original_locations->clear();
  reducer_input_bytes->clear();

  vector<string> reducer_info;
  int num_fields = split(reducer_trace, reducer_info, ',');
  if (num_fields != num_reducer_wanted) {
    cout << __func__ << ": num_fields != num_reducer_wanted. "
         << "Return empty assignments." << endl;
    return;
  }
  for (int reducer_idx = 0; reducer_idx < num_reducer_wanted; reducer_idx++) {
    vector<string> reducer_input_pair;
    int num_sub_fields = split(reducer_info[reducer_idx],
                               reducer_input_pair, ':');
    if (num_sub_fields != 2) {
      cout << __func__ << ": num_sub_fields != 2. Return empty assignments.\n";
      return;
    }
    original_locations->push_back(std::stoi(reducer_input_pair[0]));
    reducer_input_bytes->push_back(1000000 * stol(reducer_input_pair[1]));
  }
}

void TGWorstFitPlacement::PlaceTasks(
    int num_map, int num_red,
    const map<pair<int, int>, long> &mr_flow_bytes,
    const vector<int> &mapper_original_locations,
    const vector<int> &reducer_original_locations,
    vector<int> *mapper_locations,
    vector<int> *reducer_locations) {

  if (!usage_monitor_) {
    cout << "Error: TGWorstFitPlacement relies on usage monitor to work. "
         << "Must enable usage_monitor_ first. \n";
    exit(-1);
  }
  // Obtain snapshot of the current usage.
  map<int, double> expected_send_load_byte = usage_monitor_->GetSendLoadByte();
  map<int, double> expected_recv_load_byte = usage_monitor_->GetRecvLoadByte();

  // Calculate traffic load requested on mapper and reducer nodes.
  vector<double> mapper_traffic_req_MB, reducer_traffic_req_MB;
  GetNodeReqTrafficMB(num_map, num_red, mr_flow_bytes,
                      &mapper_traffic_req_MB, &reducer_traffic_req_MB);

  set<int> exclude_these_mapper_nodes; // All mappers are on different nodes
  for (int mapper_idx = 0; mapper_idx < num_map; mapper_idx++) {
    mapper_locations->push_back(
        GetNodeWithMinLoad(vector<int>(ALL_RACKS, ALL_RACKS + NUM_RACKS),
                           &exclude_these_mapper_nodes,
                           mapper_traffic_req_MB[mapper_idx] * 1e6,
                           &expected_send_load_byte));
  }
  // 2/2 place reducer tasks
  set<int> exclude_these_reducer_nodes; // All reducers are on different nodes
  if (num_map + num_red <= NUM_RACKS) {
    // For small coflows, we further asks all mappers and reducers are on
    // different nodes.
    exclude_these_reducer_nodes = exclude_these_mapper_nodes;
  }
  for (int reducer_idx = 0; reducer_idx < num_red; reducer_idx++) {
    reducer_locations->push_back(
        GetNodeWithMinLoad(vector<int>(ALL_RACKS, ALL_RACKS + NUM_RACKS),
                           &exclude_these_reducer_nodes,
                           reducer_traffic_req_MB[reducer_idx] * 1e6,
                           &expected_recv_load_byte));
  }
}

void TGTraceFB::GetNodeReqTrafficMB(
    int num_map, int num_red,
    const map<pair<int, int>, long> &mr_flow_bytes,
    vector<double> *mapper_traffic_req_MB,
    vector<double> *reducer_traffic_req_MB) {
  std::vector<double>(num_map).swap(*mapper_traffic_req_MB);
  std::vector<double>(num_red).swap(*reducer_traffic_req_MB);
  for (const auto &kv_pair: mr_flow_bytes) {
    double req_MB = (double) kv_pair.second / 1e6;
    int mapper_idx = kv_pair.first.first;
    mapper_traffic_req_MB->operator[](mapper_idx) += req_MB;
    int reducer_idx = kv_pair.first.second;
    reducer_traffic_req_MB->operator[](reducer_idx) += req_MB;
  }

}

int TGWorstFitPlacement::GetNodeWithMinLoad(const vector<int> &candidates,
                                            set<int> *exclude_these_nodes,
                                            double additional_load,
                                            map<int, double> *expected_load) {
  set<int> usable_nodes(candidates.begin(), candidates.end());
  for (int exclude : *exclude_these_nodes) {
    usable_nodes.erase(exclude);
  }
  if (usable_nodes.empty()) {
    // we read here due to a bug
    cerr << "size of exclude_these_nodes >= NUM_RACKS = " << NUM_RACKS << endl;
    cerr << "there is no way to find a valid node to place." << endl;
    exit(-1);
  }

  int assigned = 0;
  double min_load = -1;
  for (int loc : usable_nodes) {
    if (!ContainsKey(*expected_load, loc)) {
      // the usage of some nodes are not available, i.e. they are NOT utilized.
      // Find the first un-utilized node and place the task there!
      assigned = loc;
      break; // for loc
    }
    double load = expected_load->operator[](loc);
    if (load < min_load || min_load < 0) {
      min_load = load;
      assigned = loc;
    }
  }
  MapWithInc(*expected_load, assigned, additional_load);
  // force to spread workload on different nodes.
  exclude_these_nodes->insert(assigned);
  return assigned;
}

void TG2DPlacement::PlaceTasks(
    int num_map, int num_red,
    const map<pair<int, int>, long> &mr_flow_bytes,
    const vector<int> &mapper_original_locations,
    const vector<int> &reducer_original_locations,
    vector<int> *mapper_locations,
    vector<int> *reducer_locations) {
  if (!usage_monitor_) {
    cout << "Error: TGWorstFitPlacement relies on usage monitor to work. "
         << "Must enable usage_monitor_ first. \n";
    exit(-1);
  }
  // Obtain snapshot of the current usage.
  map<int, double> expected_send_load_byte = usage_monitor_->GetSendLoadByte();
  map<int, double> expected_recv_load_byte = usage_monitor_->GetRecvLoadByte();

  // Calculate traffic load requested on mapper and reducer nodes.
  vector<double> mapper_traffic_req_MB, reducer_traffic_req_MB;
  GetNodeReqTrafficMB(num_map, num_red, mr_flow_bytes,
                      &mapper_traffic_req_MB, &reducer_traffic_req_MB);

  vector<pair<int, double>> sorted_mappers, sorted_reducers;
  for (int mapper_idx = 0; mapper_idx < num_map; mapper_idx++) {
    sorted_mappers.push_back(
        std::make_pair(mapper_idx, mapper_traffic_req_MB[mapper_idx]));
  }
  std::stable_sort(sorted_mappers.begin(), sorted_mappers.end(),
                   TrafficReqDesc);

  for (int reducer_idx = 0; reducer_idx < num_red; reducer_idx++) {
    sorted_reducers.push_back(
        std::make_pair(reducer_idx, reducer_traffic_req_MB[reducer_idx]));
  }
  std::stable_sort(sorted_reducers.begin(), sorted_reducers.end(),
                   TrafficReqDesc);

  set<int> exclude_these_mapper_nodes; // All mappers are on different nodes
  map<int, int> mapper_idx_to_loc, reducer_idx_to_loc;
  for (const auto &mapper_idx_req_MB_pair: sorted_mappers) {
    int mapper_idx = mapper_idx_req_MB_pair.first;
    mapper_idx_to_loc[mapper_idx] =
        GetNodeWithMinLoad(vector<int>(ALL_RACKS, ALL_RACKS + NUM_RACKS),
                           &exclude_these_mapper_nodes,
                           mapper_traffic_req_MB[mapper_idx] * 1e6,
                           &expected_send_load_byte);
  }
  // 2/2 place reducer tasks
  set<int> exclude_these_reducer_nodes; // All reducers are on different nodes
  if (num_map + num_red <= NUM_RACKS) {
    // For small coflows, we further asks all mappers and reducers are on
    // different nodes.
    exclude_these_reducer_nodes = exclude_these_mapper_nodes;
  }
  for (const auto &reducer_idx_req_MB_pair : sorted_reducers) {
    int reducer_idx = reducer_idx_req_MB_pair.first;
    reducer_idx_to_loc[reducer_idx] =
        GetNodeWithMinLoad(vector<int>(ALL_RACKS, ALL_RACKS + NUM_RACKS),
                           &exclude_these_reducer_nodes,
                           reducer_traffic_req_MB[reducer_idx] * 1e6,
                           &expected_recv_load_byte);
  }

  assert(mapper_idx_to_loc.size() == num_map);
  assert(reducer_idx_to_loc.size() == num_red);
  for (const auto &idx_loc : mapper_idx_to_loc) {
    mapper_locations->push_back(idx_loc.second);
  }
  for (const auto &idx_loc : reducer_idx_to_loc) {
    reducer_locations->push_back(idx_loc.second);
  }
}

map<int, pair<double, int>> TGNeat::GetNeatCostFuncFromQueue(
    long alpha_cutoff, bool send_direction,
    const map<int, vector<Coflow *>> &node_queue_map) {
  map<int, pair<double, int>> neat_cost_func_MB;
  for (const auto &node_queue: node_queue_map) {
    for (Coflow *coflow : node_queue.second) {
      // higher priority with lower alpha
      if (coflow->GetStaticAlpha() < alpha_cutoff) {
        // we have flows before us.
        if (send_direction) {
          neat_cost_func_MB[node_queue.first].first +=
              coflow->GetSendLoadMB(node_queue.first);
        } else {
          neat_cost_func_MB[node_queue.first].first +=
              coflow->GetRecvLoadMB(node_queue.first);
        }
      } else {
        neat_cost_func_MB[node_queue.first].second += 1;
      }
    }
  }
  return neat_cost_func_MB;
}

map<int, pair<double, int>> TGNeat::GetNeatCostFuncFromQueue(
    long alpha_cutoff, const map<int, vector<Flow *>> &node_queue_map) {
  // update alpha based on the fresh data size.
  for (JobDesc *job : m_runningJob) {
    job->m_coflow->CalcAlpha();
  }
  map<int, pair<double, int>> neat_cost_func_MB;
  set<Coflow *> coflows_after_me;
  for (const auto &node_queue: node_queue_map) {
    for (Flow *flow : node_queue.second) {
      // higher priority with lower alpha
      // if (flow->GetParentCoflow()->GetStaticAlpha() < alpha_cutoff) {
      if (flow->GetParentCoflow()->GetAlpha() < alpha_cutoff) {
        // we have flows before us.
        neat_cost_func_MB[node_queue.first].first +=
            (double) flow->GetBitsLeft() / 8.0 / 1e6;
      } else {
        coflows_after_me.insert(flow->GetParentCoflow());
      }
    }
    neat_cost_func_MB[node_queue.first].second += coflows_after_me.size();
  }
  return neat_cost_func_MB;
}

void TGNeat::GetNeatAdjustedCostFuncMB(
    long alpha_cutoff,
    map<int, pair<double, int>> *send_cost_func,
    map<int, pair<double, int>> *recv_cost_func) {

  // more accurate based on current flow states.
  *send_cost_func = GetNeatCostFuncFromQueue(
      alpha_cutoff, usage_monitor_->GetSendFlowQueues());
  *recv_cost_func = GetNeatCostFuncFromQueue(
      alpha_cutoff, usage_monitor_->GetRecvFlowQueues());

  // less accurate based on Coflow initial states.
  //  *send_cost_func = GetNeatCostFuncFromQueue(
  //      alpha_cutoff, true/*send_direction*/,
  //      usage_monitor_->GetSendCoflowQueues());
  //  *recv_cost_func = GetNeatCostFuncFromQueue(
  //      alpha_cutoff, false/*send_direction*/,
  //      usage_monitor_->GetRecvCoflowQueues());
}

int TGNeat::GetNodeWithNeat(const map<int, vector<Flow *>> &node_queue_map,
                            double add_load_MB,
                            set<int> *exclude_these_nodes,
                            map<int, pair<double, int>> *cost_func_MB) {
  // calculate the cost for each node to place this flow onto the node.
  map<int, double> neat_node_load_MB;
  for (const auto &kv_pair : *cost_func_MB) {
    int node = kv_pair.first;
    double base = kv_pair.second.first;
    int factor = kv_pair.second.second;
    neat_node_load_MB[node] += (add_load_MB * factor + base);
  }

  // find out the preferred hosts, if any.
  // We only need to consider the flows from all other Coflows.
  // Flows from the same coflow, if they even takes a node as the sender
  // (or receiver), that node will be NOT be considered for another mapper task
  // (or reducer task).
  // host is preferred if all flows from other coflow  > current flow.
  // Remove excluding hosts from preferred hosts. If there is no preferred hosts
  // left, fall back to use ALL_RACKS.
  map<int, double> node_to_min_aggregated_coflow_load_MB;
  for (const auto &loc_queue_pair:node_queue_map) {
    map<Coflow *, double> coflow_to_aggregated_load_MB;
    for (Flow *flow : loc_queue_pair.second) {
      coflow_to_aggregated_load_MB[flow->GetParentCoflow()] +=
          (double) flow->GetBitsLeft() / 8.0 / 1e6;
    }
    if (!coflow_to_aggregated_load_MB.empty()) {
      node_to_min_aggregated_coflow_load_MB[loc_queue_pair.first] =
          MinMap(coflow_to_aggregated_load_MB);
    }
  }
  vector<int> preferred;
  for (int i = 0; i < NUM_RACKS; i++) {
    int loc = ALL_RACKS[i];
    if (ContainsKey(*exclude_these_nodes, loc)) continue; // with next loc
    if (!ContainsKey(node_to_min_aggregated_coflow_load_MB, loc) ||
        node_to_min_aggregated_coflow_load_MB[loc] >= add_load_MB) {
      preferred.push_back(loc);
    }
  }
  // cout << "Neat finds " << preferred.size() << " preferred hosts\n";
  if (preferred.empty()) {
    // fall back to consider all hosts.
    preferred = vector<int>(ALL_RACKS, ALL_RACKS + NUM_RACKS);
  }
  // If preferred != empty, preferred minus exclude_these_nodes must != empty.
  int assigned = GetNodeWithMinLoad(preferred, exclude_these_nodes,
                                    add_load_MB, &neat_node_load_MB);
  cost_func_MB->operator[](assigned).second += 1;
  return assigned;
}

void
TGNeat::PlaceTasks(int num_map, int num_red,
                   const map<pair<int, int>, long> &mr_flow_bytes,
                   const vector<int> &mapper_original_locations, // useless
                   const vector<int> &reducer_original_locations, // useless
                   vector<int> *mapper_locations,
                   vector<int> *reducer_locations) {
  if (!usage_monitor_) {
    cout << "Error: TGWorstFitPlacement relies on usage monitor to work. "
         << "Must enable usage_monitor_ first. \n";
    exit(-1);
  }

  // obtain requested flows and sort them. Larger flows are in the front.
  vector<pair<pair<int, int>, long>> sorted_mr_flow_bytes;
  for (const auto &mr_flow : mr_flow_bytes) {
    sorted_mr_flow_bytes.push_back(mr_flow);
  }
  if (use_ascending_req_) {
    std::stable_sort(sorted_mr_flow_bytes.begin(), sorted_mr_flow_bytes.end(),
                     CompByFlowValueAsce);
  } else {
    std::stable_sort(sorted_mr_flow_bytes.begin(), sorted_mr_flow_bytes.end(),
                     CompByFlowValueDesc);
  }
  // Aggregate the network IO load on mappers and reducers.
  vector<long> mapper_traffic_req_byte = vector<long>(num_map, 0.0);
  vector<long> reducer_traffic_req_byte = vector<long>(num_red, 0.0);
  for (const auto &kv_pair: mr_flow_bytes) {
    double req_byte = (double) kv_pair.second;
    int mapper_idx = kv_pair.first.first;
    mapper_traffic_req_byte[mapper_idx] += req_byte;
    int reducer_idx = kv_pair.first.second;
    reducer_traffic_req_byte[reducer_idx] += req_byte;
  }
  long bottleneck_byte = max(
      *std::max_element(mapper_traffic_req_byte.begin(),
                        mapper_traffic_req_byte.end()),
      *std::max_element(reducer_traffic_req_byte.begin(),
                        reducer_traffic_req_byte.end()));
  long current_alpha = bottleneck_byte * 8;
  map<int, pair<double, int>> send_cost_func_MB, recv_cost_func_MB;
  GetNeatAdjustedCostFuncMB(current_alpha,
                            &send_cost_func_MB, &recv_cost_func_MB);

  // try to spread the coflow. small coflows require all mappers and reducers
  // are on different nodes. large coflows require distinct mappers (reducers).
  set<int> *mapper_exclude, *reducer_exclude;
  set<int> exclude_these_mapper_nodes, exclude_these_reducer_nodes;
  if (num_map + num_red <= NUM_RACKS) {
    // For samll coflow that can spread across all racks, reuse the same set so
    // that mapper and reducer placements are mutually exclusive.
    mapper_exclude = &exclude_these_mapper_nodes;
    reducer_exclude = &exclude_these_mapper_nodes; // same pool
  } else {
    // For large coflow that requires some mapper and reducer to share nodes
    mapper_exclude = &exclude_these_mapper_nodes;
    reducer_exclude = &exclude_these_reducer_nodes; // different pool
  }

  // Begin placement by considering flows from large to small
  map<int, int> mapper_idx_to_loc, reducer_idx_to_loc;
  for (const auto &flow_info : sorted_mr_flow_bytes) {
    int mapper_idx = flow_info.first.first;
    int reducer_idx = flow_info.first.second;
    // place mapper if needed.
    if (!ContainsKey(mapper_idx_to_loc, mapper_idx)) {
      mapper_idx_to_loc[mapper_idx] = GetNodeWithNeat(
          usage_monitor_->GetSendFlowQueues(),
          (double) mapper_traffic_req_byte[mapper_idx] / 1e6 /*add_load_MB*/,
          mapper_exclude, &send_cost_func_MB);
      //TODO: remove debug message
//      int assigned = mapper_idx_to_loc[mapper_idx];
//      cout << "TGNeat places mapper #" << mapper_idx << " /" << num_map
//           << " on node " << assigned
//           << ", cost func after placement "
//           << "(base=" << send_cost_func_MB[assigned].first << " MB, "
//           << "factor= " << send_cost_func_MB[assigned].second << ")\n";
    }
    // place reducer if needed.
    if (!ContainsKey(reducer_idx_to_loc, reducer_idx)) {
      reducer_idx_to_loc[reducer_idx] = GetNodeWithNeat(
          usage_monitor_->GetRecvFlowQueues(),
          (double) reducer_traffic_req_byte[reducer_idx] / 1e6 /*add_load_MB*/,
          reducer_exclude, &recv_cost_func_MB);
      //TODO: remove debug message
//      int assigned = reducer_idx_to_loc[reducer_idx];
//      cout << "TGNeat places reducer #" << reducer_idx << " /" << num_red
//           << " on node " << assigned
//           << ", cost func after placement "
//           << "(base=" << recv_cost_func_MB[assigned].first << " MB, "
//           << "factor= " << recv_cost_func_MB[assigned].second << ")\n";
    }
    // Done with placing all mappers and reducers.
    if (mapper_idx_to_loc.size() >= num_map
        && reducer_idx_to_loc.size() >= num_red) {
      break; // from for flow_info
    }
  }

  assert(mapper_idx_to_loc.size() == num_map);
  assert(reducer_idx_to_loc.size() == num_red);
  for (const auto &idx_loc : mapper_idx_to_loc) {
    mapper_locations->push_back(idx_loc.second);
  }
  for (const auto &idx_loc : reducer_idx_to_loc) {
    reducer_locations->push_back(idx_loc.second);
  }
}

// use seed_for_seed to initialize the vector of m_seed_for_coflow,
// in length of PERTURB_SEED_NUM.
void TGTraceFB::InitSeedForCoflows(int seed_for_seed) {
  std::mt19937 mt_rand(seed_for_seed); //  srand(seed_for_seed);
  for (int i = 0; i < PERTURB_SEED_NUM; i++) {
    m_seed_for_coflow.push_back(mt_rand()); // m_seed_for_coflow.push_back(rand());
    // cout << " seed [" << i << "] = "
    //      << m_seed_for_coflow.back() << endl;
  }
}

int
TGTraceFB::GetSeedForCoflow(int coflow_id) {
  // use jobid to identify a unique seed;
  int seed_idx = (coflow_id + 5122) % PERTURB_SEED_NUM;
  int seed = m_seed_for_coflow[seed_idx];
  // cout << "job id " << jobid << " seed " << seed << endl;
  return seed;
}

// sum( flows to a reducer ) == reducer's input specified in redInput.
map<pair<int, int>, long>
TGTraceFB::GetFlowSizeWithExactSize(int numMap,
                                    int numRed,
                                    const vector<long> &redInput) {
  map<pair<int, int>, long> mr_flow_bytes_result;
  for (int reducer_idx = 0; reducer_idx < numRed; reducer_idx++) {
    long redInputTmp = redInput[reducer_idx];
    long avgFlowSize = ceil((double) redInputTmp / (double) numMap);
    for (int mapper_idx = 0; mapper_idx < numMap; mapper_idx++) {
      long flowSize = min(avgFlowSize, redInputTmp);
      redInputTmp -= flowSize;
      mr_flow_bytes_result[std::make_pair(mapper_idx, reducer_idx)] = flowSize;
    }
  }
  return mr_flow_bytes_result;
}

// divide reducer's input size, specified in redInput, to each of the mapper.
map<pair<int, int>, long>
TGTraceFB::GetFlowSizeWithEqualSizeToSameReducer(int numMap,
                                                 int numRed,
                                                 const vector<long> &redInput) {
  map<pair<int, int>, long> mr_flow_bytes_result;
  for (int reducer_idx = 0; reducer_idx < numRed; reducer_idx++) {
    long avgFlowSize = ceil((double) redInput[reducer_idx] / (double) numMap);
    for (int mapper_idx = 0; mapper_idx < numMap; mapper_idx++) {
      long flowSize = avgFlowSize;
      mr_flow_bytes_result[std::make_pair(mapper_idx, reducer_idx)] = flowSize;
    }
  }
  return mr_flow_bytes_result;
}

map<pair<int, int>, long>
TGTraceFB::GetFlowSizeWithPerturb(int numMap,
                                  int numRed,
                                  const vector<long> &redInput,
                                  int perturb_perc,
                                  unsigned int rand_seed) {

  if (perturb_perc > 100) {
    cout << " Warming : try to perturb the flow sizes with more than 1MB \n";
  }
  // seed the random generator before we preturb,
  // so that given same traffic trace,
  // we will have the same traffic for different schedulers.
  std::mt19937 mt_rand(rand_seed); // srand(rand_seed);

  map<pair<int, int>, long> mr_flow_bytes_result;
  // now we generate traffic.
  for (int reducer_idx = 0; reducer_idx < numRed; reducer_idx++) {
    long redInputTmp = redInput[reducer_idx];

    long avgFlowSize = ceil((double) redInputTmp / (double) numMap);

    for (int mapper_idx = 0; mapper_idx < numMap; mapper_idx++) {

      int perturb_direction = (mt_rand() % 2 == 1) ? 1 : -1;
      // int perturb_direction = (rand() % 2 == 1) ? 1 : -1;

      // perturb_perc = 5 : (-5%, +5%) flow size , exclusive bound

      double rand_0_to_1 = ((double) mt_rand() / (RAND_MAX));
      // double rand_0_to_1 = ((double) rand() / (RAND_MAX));
      double perturb_perc_rand =
          perturb_direction * rand_0_to_1 * (double) perturb_perc / 100.0;
      long flowSize = avgFlowSize * (1 + perturb_perc_rand);

      // only allow flows >= 1MB.
      if (flowSize < 1000000) flowSize = 1000000;
      redInputTmp -= flowSize;
      mr_flow_bytes_result[std::make_pair(mapper_idx, reducer_idx)] = flowSize;

      // debug
      if (DEBUG_LEVEL >= 10) {
        cout << "mapper " << mapper_idx << " -> reducer " << reducer_idx << ", "
             << "flow size after perturb " << flowSize << ", "
             << "avgFlowSize " << avgFlowSize << ", "
             << "perturb_perc_rand " << perturb_perc_rand << endl;
      }
    }
  }
  return mr_flow_bytes_result;
}