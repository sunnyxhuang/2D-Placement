//
// Created by Xin Huang on 3/13/17.
//

#ifndef XIMULATOR_USAGE_MONITOR_H
#define XIMULATOR_USAGE_MONITOR_H

#include "coflow.h"
#include "db_logger.h"

//
// Monitor resource usage based coflow/flow arrival and departure.
class UsageMonitor {
 public:
  UsageMonitor(DbLogger *db_logger);
  ~UsageMonitor();
  // Register all flows within a coflow upon coflow arrival. This assumes tasks
  // does not start and use resource until coflow arrives. But such assumpiton
  // about task resource consumption may not hold true in reality.
  // TODO: Refine this assumption if necessary.
  void Register(double time, Coflow *coflow);
  // Unregister a flow when the flow finishes, which may release the resource
  // usage on the src/dst machine.
  void Unregister(double time, Coflow *coflow, Flow *flow);
  // Unregister a coflow. All internal records related to this coflow will be
  // removed.
  void Unregister(double time, Coflow *coflow);

  // For WorstFit: Getters of node load for network I/O
  const map<int, double> &GetSendLoadByte() { return send_load_byte_; }
  const map<int, double> &GetRecvLoadByte() { return recv_load_byte_; }

  // For NEAT: Getters of flow queues
  const map<int, vector<Flow *>> &GetIOFlowQueues() { return IO_flow_queues_; }
  const map<int, vector<Flow *>> &GetSendFlowQueues() {
    return send_flow_queues_;
  }
  const map<int, vector<Flow *>> &GetRecvFlowQueues() {
    return recv_flow_queues_;
  }

  // For NEAT: Getters of coflow queues
  const map<int, vector<Coflow *>> &GetSendCoflowQueues() {
    return send_coflow_queues_;
  }
  const map<int, vector<Coflow *>> &GetRecvCoflowQueues() {
    return recv_coflow_queues_;
  }


 private:

  // used for accounting of resource usage.
  // map from Coflow pointer to the flow count from/to mapper/reducer.
  // each flow count is another map from node index of mapper/reducer to its
  // outbound/inbound flow count.
  // New entry for a coflow is inserted when Register() is called, and the flow
  // count are initialized to reflect all flows in the registered coflow.
  // Flow count is decreased when a flow is completed and Unregister() is
  // called. If the flow count for the mapper/reducer reaches 0, the resource
  // used by the mapper/reducer node is released.
  // Removed an entry for a coflow if the Coflow is done and
  map<Coflow *, map<int, int>> mapper_flow_count_;
  map<Coflow *, map<int, int>> reducer_flow_count_;

  // used for min_load (NEAT paper in CoNEXT'16) based on traffic size in MB.
  // map from node index to the total size of all flows active on the node.
  // The load on a node is increased upon Register() when flows arrive and
  // decreased upon Unregister(double, Flow*, Coflow*) when a flow finishes.
  // Caution: Unregister(Coflow*) on the whole Coflow will NOT update this
  // record. The user must use Unregister(double, Flow*, Coflow*) to reduce the
  // node load record.

  map<int, double> send_load_byte_; // map from node to node load as the sender
  map<int, double> recv_load_byte_; // ...  as the receiver

  // Used for NEAT algorithm (NEAT paper in CoNEXT'16).
  // Map from node index to a vector of flows active on the node. Each queue is
  // maintained as a priority non-decreasing vector.
  // The queues are updated upon Register() and Unregister(flow). We improve
  // over the original assumption of NEAT, which assumes all flows are finished
  // at the same time when the coflow finishes.
  // Here we provide a bit more accurate flow queues which are also updated upon
  // flow completion, but we still assume the inter-coflow (i.e. inter-flow)
  // priority remains the same over time.

  // In this queue, ALL inbound and outbound flows are considered, i.e. we do
  // NOT distinguish the direction of flows on the queue.
  map<int, vector<Flow *>> IO_flow_queues_;

  // src_flow_queues_ only considers the flows use the node as sender (mapper).
  // dst_flow_queues_ ... as receiver (reducer)
  map<int, vector<Flow *>> send_flow_queues_;
  map<int, vector<Flow *>> recv_flow_queues_;

  // The following *coflow* queues are are updated upon Register() and
  // Unregister(coflow). No updates upon Unregister(flow).
  map<int, vector<Coflow *>> send_coflow_queues_;
  map<int, vector<Coflow *>> recv_coflow_queues_;

  // Manage flow queues.
  // In our coflow scheduling problem, all flows in a coflow share the same
  // priority, which equals to the coflow's priority. Because NEAT assumes all
  // flows are finished at the same time when the coflow finishes, as a result,
  // the coflow priority is coflow's original bottleneck and not changed over
  // time even if the inter-coflow priority may change as their bottlenecks
  // shrinks.
  // The the bottleneck (alpha) is, the priority is higher.
  void InsertPrioritizedFirst(std::vector<Flow *> *queue, Flow *new_flow);
  void RemoveFlowFromQueue(std::vector<Flow *> *queue, Flow *flow);
  // Manage coflow queues.
  void InsertPrioritizedFirst(std::vector<Coflow *> *queue, Coflow *new_coflow);
  void RemoveCoflowFromQueue(std::vector<Coflow *> *queue, Coflow *coflow);

  DbLogger *db_logger_; // Not owned.
};

#endif //XIMULATOR_USAGE_MONITOR_H
