#include <algorithm>

#include "usage_monitor.h"
#include "global.h"


UsageMonitor::UsageMonitor(DbLogger *db_logger) {
  if (!db_logger) {
    cout << "Warning: db logging is disabled. "
         << "But still measuring resource usage " << endl;
    // exit(-1);
  }
  db_logger_ = db_logger;

}

UsageMonitor::~UsageMonitor() {
  if (!mapper_flow_count_.empty() || !reducer_flow_count_.empty()) {
    cerr << "ERROR: expect all coflows are unregistered before UsageMonitor "
         << "is destroyed. But mapper_flow_count_ has "
         << mapper_flow_count_.size() << " records left, "
         << "and reducer_flow_count_ has "
         << reducer_flow_count_.size() << " records left. " << endl;
    // clean up remaining coflows.
    double INF_TIME = 1e8;
    std::set<Coflow *> remaining_coflows;
    for (const auto &kv_pair:mapper_flow_count_) {
      remaining_coflows.insert(kv_pair.first);
    }
    for (const auto &kv_pair:reducer_flow_count_) {
      remaining_coflows.insert(kv_pair.first);
    }
    for (Coflow *coflow:remaining_coflows) {
      Unregister(INF_TIME, coflow);
    }
  }
}

void UsageMonitor::Register(double time, Coflow *coflow) {
  if (DEBUG_LEVEL >= 5) {
    cout << "[UsageMonitor::Register] " << time << "s: "
         << "Going to add coflow " << coflow->GetJobId() << endl;
  }
  // The coflow should be first seen.
  if (ContainsKey(mapper_flow_count_, coflow)
      || ContainsKey(reducer_flow_count_, coflow)) {
    cerr << "ERROR: coflow " << coflow->GetJobId() << " has been registered!\n";
    return;
  }

  // Accouting flow count.
  map<int, int> &mapper_flow_num = mapper_flow_count_[coflow];
  map<int, int> &reducer_flow_num = reducer_flow_count_[coflow];
  for (Flow *flow : *coflow->GetFlows()) {
    mapper_flow_num[flow->GetSrc()]++;
    reducer_flow_num[flow->GetDest()]++;
  }

  // Accounting (inc ++) for netIO load on nodes.
  for (Flow *flow : *coflow->GetFlows()) {
    double size_byte = (double) flow->GetSizeInBit() / 8.0;
    send_load_byte_[flow->GetSrc()] += size_byte;
    recv_load_byte_[flow->GetDest()] += size_byte;
  }
  // Accouting (inc ++) for flow queue.
  for (Flow *flow : *coflow->GetFlows()) {
    // update NON-directional (mixed) queues.
    InsertPrioritizedFirst(&IO_flow_queues_[flow->GetSrc()], flow);
    InsertPrioritizedFirst(&IO_flow_queues_[flow->GetDest()], flow);

    // update directional queues.
    InsertPrioritizedFirst(&send_flow_queues_[flow->GetSrc()], flow);
    InsertPrioritizedFirst(&recv_flow_queues_[flow->GetDest()], flow);
  }

  // Accouting (inc++) for coflow queue.
  for (int mapper_node : coflow->GetMapperLocations()) {
    InsertPrioritizedFirst(&send_coflow_queues_[mapper_node], coflow);
  }
  for(int reducer_node : coflow->GetReducerLocations()) {
    InsertPrioritizedFirst(&recv_coflow_queues_[reducer_node], coflow);
  }
}

void UsageMonitor::Unregister(double time, Coflow *coflow) {
  if (DEBUG_LEVEL >= 5) {
    cout << "[UsageMonitor::Unregister] " << time << "s: Going to remove "
         << coflow->GetJobId() << endl;
  }
  if (!ContainsKey(mapper_flow_count_, coflow)
      || !ContainsKey(reducer_flow_count_, coflow)) {
    cerr << "ERROR: coflow " << coflow->GetJobId() << " is not registered!\n";
    return;
  }
  map<int, int> &mapper_flow_num = mapper_flow_count_[coflow];
  map<int, int> &reducer_flow_num = reducer_flow_count_[coflow];

  mapper_flow_count_.erase(coflow);
  reducer_flow_count_.erase(coflow);

  // Accouting (dec --) for coflow queue.
  for (int mapper_node : coflow->GetMapperLocations()) {
    RemoveCoflowFromQueue(&send_coflow_queues_[mapper_node], coflow);
  }
  for(int reducer_node : coflow->GetReducerLocations()) {
    RemoveCoflowFromQueue(&recv_coflow_queues_[reducer_node], coflow);
  }
}

void UsageMonitor::Unregister(double time, Coflow *coflow, Flow *flow) {
  if (DEBUG_LEVEL >= 5) {
    cout << "[UsageMonitor::Unregister] " << time << "s: "
         << "Going to unregister flow " << flow->GetFlowId()
         << " (" << flow->GetSrc() << "->" << flow->GetDest() << ")"
         << " from coflow " << coflow->GetJobId() << endl;
  }
  if (!ContainsKey(mapper_flow_count_, coflow)
      || !ContainsKey(reducer_flow_count_, coflow)) {
    cerr << "ERROR: coflow " << coflow->GetJobId() << " is not registered!\n";
    return;
  }
  if (find(coflow->GetFlows()->begin(), coflow->GetFlows()->end(), flow)
      == coflow->GetFlows()->end()) {
    cerr << "Flow " << flow->GetFlowId()
         << " (" << flow->GetSrc() << "->" << flow->GetDest() << ") "
         << "is not in coflow " << coflow->GetJobId() << endl;
    return;
  }

  map<int, int> &mapper_flow_num = mapper_flow_count_[coflow];
  map<int, int> &reducer_flow_num = reducer_flow_count_[coflow];
  int mapper_loc = flow->GetSrc();
  int reducer_loc = flow->GetDest();
  if (!ContainsKey(mapper_flow_num, mapper_loc)
      || !ContainsKey(reducer_flow_num, reducer_loc)) {
    cerr << "ERROR: coflow " << coflow->GetJobId()
         << " does not have flow with id " << flow->GetFlowId() << ", "
         << mapper_loc << "->" << reducer_loc << endl;
    return;
  }

  if (mapper_flow_num[mapper_loc] <= 0) {
    cerr << "ERROR: coflow " << coflow->GetJobId() << " at mapper "
         << mapper_loc << " does not have any flow left. " << endl;
    return;
  }
  if (reducer_flow_num[reducer_loc] <= 0) {
    cerr << "ERROR: coflow " << coflow->GetJobId() << " at reducer "
         << reducer_loc << " does not have any flow left. " << endl;
    return;
  }

  mapper_flow_num[mapper_loc]--;
  reducer_flow_num[reducer_loc]--;

  // Accounting (dec --) for netIO load on nodes.
  double size_byte = (double) flow->GetSizeInBit() / 8.0;
  send_load_byte_[flow->GetSrc()] -= size_byte;
  recv_load_byte_[flow->GetDest()] -= size_byte;

  // Accouting (dec --) for flow quue;
  RemoveFlowFromQueue(&IO_flow_queues_[flow->GetSrc()], flow);
  RemoveFlowFromQueue(&IO_flow_queues_[flow->GetDest()], flow);

  // update directional queues.
  RemoveFlowFromQueue(&send_flow_queues_[flow->GetSrc()], flow);
  RemoveFlowFromQueue(&recv_flow_queues_[flow->GetDest()], flow);
}

void UsageMonitor::InsertPrioritizedFirst(std::vector<Flow *> *queue,
                                          Flow *new_flow) {
  std::vector<Flow *>::iterator iter;
  for (iter = queue->begin(); iter != queue->end(); iter++) {
    if (new_flow->GetParentCoflow()->GetStaticAlpha()
        < (*iter)->GetParentCoflow()->GetStaticAlpha()) {
      break;
    }
  }
  queue->insert(iter, new_flow);
}

void UsageMonitor::RemoveFlowFromQueue(std::vector<Flow *> *queue, Flow *flow) {
  vector<Flow *>::iterator iter;
  for (iter = queue->begin(); iter != queue->end(); iter++) {
    if (flow == *iter) {
      queue->erase(iter);
      break;
    }
  }
}


void UsageMonitor::InsertPrioritizedFirst(std::vector<Coflow *> *queue,
                                          Coflow *new_coflow) {
  std::vector<Coflow *>::iterator iter;
  for (iter = queue->begin(); iter != queue->end(); iter++) {
    if (new_coflow->GetStaticAlpha() < (*iter)->GetStaticAlpha()) {
      break;
    }
  }
  queue->insert(iter, new_coflow);
}

void UsageMonitor::RemoveCoflowFromQueue(std::vector<Coflow *> *queue,
                                         Coflow *coflow) {
  vector<Coflow *>::iterator iter;
  for (iter = queue->begin(); iter != queue->end(); iter++) {
    if (coflow == *iter) {
      queue->erase(iter);
      break;
    }
  }
}
