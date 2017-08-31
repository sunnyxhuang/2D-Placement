#include "global.h"
#include "util.h"

// Some useful constant/scaler
//const double ONE_GIGA_DOUBLE = 1.0e9;
const long ONE_GIGA_LONG = 1e9;
// const long TEN_GIGA_LONG = 10 * 1e9;

long DEFAULT_LINK_RATE_BPS = ONE_GIGA_LONG / NUM_LINK_PER_RACK;
long ELEC_BPS = ONE_GIGA_LONG / NUM_LINK_PER_RACK; //1G = 1000000000

bool DEADLINE_MODE = false;
double DEADLINE_ERROR_TOLERANCE = 0.0001;

bool ENABLE_PERTURB_IN_PLAY = true;
// valid if ENABLE_PERTURB_IN_PLAY = false;
// all flows to the same reducer will be equally
// distributed for all mappers.
bool EQUAL_FLOW_TO_SAME_REDUCER = false;

// used to initialized end time.
double INVALID_TIME = -1.0;

// default num of racks used in tms to determine
// the bound for random selected rack to fill demand
int NUM_RACKS = 150;
int ALL_RACKS[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                   10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
                   20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
                   30, 31, 32, 33, 34, 35, 36, 37, 38, 39,
                   40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
                   50, 51, 52, 53, 54, 55, 56, 57, 58, 59,
                   60, 61, 62, 63, 64, 65, 66, 67, 68, 69,
                   70, 71, 72, 73, 74, 75, 76, 77, 78, 79,
                   80, 81, 82, 83, 84, 85, 86, 87, 88, 89,
                   90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
                   100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
                   110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
                   120, 121, 122, 123, 124, 125, 126, 127, 128, 129,
                   130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
                   140, 141, 142, 143, 144, 145, 146, 147, 148, 149};
//  NUM_LINK_PER_RACK circuits are connected for a rack;
//  rack_number = circuit_number mod NUM_RACKS
int NUM_LINK_PER_RACK = 1;

// used by traffic generator.
// which indicates max number of coflows supported per run.
// used to initialized the random seed for each coflow.
int PERTURB_SEED_NUM = 600;

// all flow byte sizes are multiplied by TRAFFIC_SIZE_INFLATE.
double TRAFFIC_SIZE_INFLATE = 1;

// TODO: replace MAC_BASE_DIR with the absolute path to your directory of the
// simulator package, e.g. "/Users/yourid/Documents/research/2D-Placement/"
string MAC_BASE_DIR = "/Users/yourid/Documents/research/2D-Placement/";
string LINUX_BASE_DIR = "../";
string BASE_DIR = IsOnApple() ? MAC_BASE_DIR : LINUX_BASE_DIR;

string TRAFFIC_TRACE_FILE_NAME = BASE_DIR + "trace/fbtrace-1hr.txt";

string RESULTS_DIR = "results/";
string TRAFFIC_AUDIT_FILE_NAME = BASE_DIR + RESULTS_DIR + "audit_traffic.txt";

bool ZERO_COMP_TIME = true;

// for aalo.
int AALO_Q_NUM = 10;
double AALO_INIT_Q_HEIGHT = 10.0 * 1000000; // 10MB
double AALO_Q_HEIGHT_MULTI = 10.0;

// a flag to indicated this coflow has inf large alpha (expected cct).
double CF_DEAD_ALPHA_SIGN = -1.0;
// consider online alpha to be infinite if longer than this cutoff,
// so that the coflow may be skipped and enter the so-called "fair-share" trick.
double ONLINE_ALPHA_CUTOFF = 10000000; // 1000000000

// by default 0 - no debug string
// more debug string with higher DEBUG_LEVEL
int DEBUG_LEVEL = 0;

const int FLOAT_TIME_WIDTH = 10;
