#include <algorithm> // transform
#include <ctgmath>
#include <string.h>
#include <unistd.h> // get hoot name

#include "util.h"

using namespace std;

string ToLower(const string& in){
  string result = in;
  transform(in.begin(), in.end(), result.begin(), ::tolower);
  return result;
}

bool IsOnApple() {
  size_t HOST_NAME_MAX = 100;
  char hostname[HOST_NAME_MAX];
  gethostname(hostname, HOST_NAME_MAX);
  return NULL != strstr(hostname, "MacBook");
}

bool IsOnDbNode() {
  size_t HOST_NAME_MAX = 100;
  char hostname[HOST_NAME_MAX];
  gethostname(hostname, HOST_NAME_MAX);
  return NULL != strstr(hostname, "bold-node001");
}

unsigned long split(const string &txt, vector<string> &strs, char ch) {
  unsigned long pos = txt.find(ch);
  unsigned long initialPos = 0;
  strs.clear();

  // Decompose statement
  while (pos != std::string::npos) {
    strs.push_back(txt.substr(initialPos, pos - initialPos));
    initialPos = pos + 1;

    pos = txt.find(ch, initialPos);
  }

  // Add the last one
  unsigned long lastPos = pos < txt.size() ? pos : txt.size();
  strs.push_back(txt.substr(initialPos, lastPos - initialPos));

  return strs.size();
}

// TODO : remove this.
long sciStringToLong(string sci_num_str) {
  double base = stod(sci_num_str);
  int power = 0;
  std::size_t power_pos = sci_num_str.find("e+");
  if (power_pos < sci_num_str.size()) {
    // has power
    stoi(sci_num_str.substr(power_pos + 2));
  }
  return base * pow(10, power);
}

// comparator that sorts items large -> small by value(second)
// maintain order if values equal.
bool CompByFlowValueDesc(std::pair<pair<int, int>, long> const &a,
                         std::pair<pair<int, int>, long> const &b) {
  return a.second > b.second;
};

bool CompByFlowValueAsce(std::pair<pair<int, int>, long> const &a,
                         std::pair<pair<int, int>, long> const &b) {
  return a.second < b.second;
};

// TODO: remove unused.
bool CompByPortValue(std::pair<int, long> const &a,
                     std::pair<int, long> const &b) {
  return a.second > b.second;
};

double secondPass(struct timeval end_time, struct timeval start_time) {
  return (double) (end_time.tv_sec - start_time.tv_sec)
      + ((double) (end_time.tv_usec - start_time.tv_usec)) / (double) 1000000;
}
 