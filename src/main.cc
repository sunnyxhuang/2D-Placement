#include <iostream>

#include "events.h"
#include "db_logger.h"

using namespace std;

int main(int argc, const char *argv[]) {

  // *- "varysImpl" : varys implemention _similar_ to that on GitHub
  //                  = selfish coflow + flow based Work Conservation
  //                  Tweaked by Sunny because the performance of the original
  //                  implementation is too bad when compared with Sunflow.
  //                  After tweaked, total/avg CCT performance is improved and
  //                  becomes comparable with Sunflow.
  //  - "aaloImpl" : aalo implemented as seen in GitHub

  string schedulerName = "varysImpl";

  TRAFFIC_SIZE_INFLATE = 1;
  DEBUG_LEVEL = 0;

  // trace replay:  "2DPlace" (our work) | "Neat" | "fb"
  // analysis only: "analyzeONLY"
  // the "*ONLY" modes are used for analysis only - no scheduler is involved.
  string trafficProducerName = "2DPlace";

  for (int i = 1; i < argc; i = i + 2) {
    /* We will iterate over argv[] to get the parameters stored inside.
     * Note that we're starting on 1 because we don't need to know the
     * path of the program, which is stored in argv[0] */
    if (i + 1 != argc) { // Check that we haven't finished parsing already
      string strFlag = string(argv[i]);
      if (strFlag == "-elec") {
        string content(argv[i + 1]);
        ELEC_BPS = stol(content);
      } else if (strFlag == "-inflate") {
        string content(argv[i + 1]);
        TRAFFIC_SIZE_INFLATE = stod(content);
      } else if (strFlag == "-traffic") {
        trafficProducerName = string(argv[i + 1]);
      } else if (strFlag == "-ftrace") {
        TRAFFIC_TRACE_FILE_NAME = string(argv[i + 1]);
      } else if (strFlag == "-faudit") {
        TRAFFIC_AUDIT_FILE_NAME = string(argv[i + 1]);
      } else if (strFlag == "-s") {
        schedulerName = string(argv[i + 1]);
      } else if (strFlag == "-zc") {
        string content(argv[i + 1]);
        ZERO_COMP_TIME = (ToLower(content) == "true");
      } else {
        cout << "invalid arguments " << strFlag << " \n";
        exit(0);
      }
    }
  }

  // DEFAULT_LINK_RATE_BPS for bottle-neck calculation
  DEFAULT_LINK_RATE_BPS = ELEC_BPS;

  bool no_scheduler = trafficProducerName.length() > 4
      && trafficProducerName.substr(trafficProducerName.length() - 4) == "ONLY";
  string db_table_subfix = no_scheduler
                           ? trafficProducerName
                           : schedulerName + "_" + trafficProducerName;
  DbLogger db_logger(db_table_subfix);

  Simulator ximulator;
  // Some configs may be changed inside traffic generator and the scheduler.
  ximulator.InstallTrafficGen(trafficProducerName, &db_logger);

  if (!no_scheduler) {
    // Unless replay mode, scheduler is useless and we do not run simulation.
    // To use RememberForget analyser, install traffic producer first.
    no_scheduler = !ximulator.InstallScheduler(schedulerName);
  }

  if (no_scheduler) {
    // no need to continue if no scheduler.
    return 0;
  }

  // Print out some important configurations.
  cout << "schedulerName = " << schedulerName << endl;
  cout << "trafficProducer = " << trafficProducerName << endl;
  cout << "TRAFFIC_SIZE_INFLATE = " << TRAFFIC_SIZE_INFLATE << endl;
  cout << "ELEC_BPS = " << ELEC_BPS << endl;
  cout << "ZERO_COMP_TIME = " << std::boolalpha << ZERO_COMP_TIME << endl;
  cout << "NUM_RACKS = " << NUM_RACKS << " *  NUM_LINK_PER_RACK = "
       << NUM_LINK_PER_RACK << endl;
  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";
  cout << "ENABLE_PERTURB_IN_PLAY = " << std::boolalpha
       << ENABLE_PERTURB_IN_PLAY << endl;
  cout << " *  *  " << endl;
  int file_name_cutoff = (int) TRAFFIC_TRACE_FILE_NAME.size() - 30;
  if (file_name_cutoff < 0) file_name_cutoff = 0;
  cout << "TRAFFIC_TRACE_FILE_NAME = "
       << TRAFFIC_TRACE_FILE_NAME.substr(file_name_cutoff) << endl;

  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";
  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";
  cout << " *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  *  * \n";

  // Unless replay mode, scheduler is useless and we do not run simulation.
  ximulator.Run();

  return 0;
}


