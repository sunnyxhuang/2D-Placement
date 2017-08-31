#include "coflow.h"
#include "events.h"
#include "traffic_generator.h"
#include "util.h"
///////////////////////////////////////////////////////////////////////////////
///////////// Code for quick analyze of coflows
///////////////////////////////////////////////////////////////////////////////

void
AnalyzeBase::LoadAllCoflows(vector<Coflow *> *load_to_me) {
  string jobLine;

  // load all coflows from trace.
  while (!m_jobTraceFile.eof()) {
    getline(m_jobTraceFile, jobLine);
    if (jobLine.size() <= 0) {
      //cout << "no more jobs are available!" << endl;
      break; // while(!m_jobTraceFile.eof())
    }

    vector<string> subFields;
    long numFields = split(jobLine, subFields, '\t');

    if (numFields != 5) {
      break; // while(!m_jobTraceFile.eof())
    }

    int jobid = stoi(subFields[0]);
    double jobOffArrivalTime = stod(subFields[1]) / 1000.0;
    int map = stoi(subFields[2]);
    int red = stoi(subFields[3]);

    // do perturb if needed.
    // when perturb = false,
    //  if EQUAL_FLOW_TO_SAME_REDUCER = true, all flows to the same reducer
    //     will be the of the same size.
    Coflow *cfp = CreateCoflowPtrFromString(jobOffArrivalTime, jobid,
                                            map, red, subFields[4],
                                            ENABLE_PERTURB_IN_PLAY,
                                            EQUAL_FLOW_TO_SAME_REDUCER);

    if (!cfp) {
      cout << "Error: cfp NULL upon create!" << endl;
      break; // while(!m_jobTraceFile.eof())
    }
    cfp->SetJobId(jobid);
    // cfp->SetNumMap(map);
    // cfp->SetNumRed(red);
    load_to_me->push_back(cfp);
    if (db_logger_) {
      db_logger_->WriteCoflowFeatures(cfp);
    }
  }
}

void
AnalyzeBase::CleanUpCoflows(vector<Coflow *> *clean_me) {
  // clean up coflow we created for analysis purposes.
  for (vector<Coflow *>::iterator coflow = clean_me->begin();
       coflow != clean_me->end();
       coflow++) {
    delete *coflow;
  }
}
