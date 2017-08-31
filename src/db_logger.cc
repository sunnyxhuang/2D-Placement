#include <unistd.h>

#include "db_logger.h"
#include "util.h"

// static
bool DbLogger::LOG_FLOW_INFO_ = false;

bool DbLogger::Connect() {
   //
  return true;
}

DbLogger::DbLogger(string table_subfix) : table_subfix_(table_subfix) {
  if (!Connect()) {
    exit(-1);
  }
  has_init_coflow_info_table_ = false;
  if (LOG_FLOW_INFO_) InitFlowInfoTable();
}

DbLogger::~DbLogger() {
  // flush all data to database if needed
}

string DbLogger::GetTableName(string tabel_type) {
  return tabel_type + (table_subfix_ == "" ? "" : "_" + table_subfix_);
}

bool DbLogger::DropIfExist(string table_type) {
  if (!Connect()) {
    return false;
  }
  string table_name = GetTableName(table_type);
  // sql to drop table. You may also write to file named TRAFFIC_AUDIT_FILE_NAME
  //  query << "drop table if exists " << table_name;
  //  if (!query.exec()) {
  //    cerr << query.str() << endl;
  //    cerr << query.error() << endl;
  //    return false;
  //  }
  //  cout << "Dropped table " << table_name << endl;
  return true;
}

void DbLogger::InitCoflowInfoTable() {
  if (has_init_coflow_info_table_) return;
  if (!DropIfExist("CoflowInfo")) return;
  string table_name = GetTableName("CoflowInfo");
  // sql to write table. You may also write to file named TRAFFIC_AUDIT_FILE_NAME
  //  query << "CREATE TABLE `" << table_name << "` (\n"
  //        << "    `job_id` INT(11) NOT NULL,\n"
  //        << "    `insert_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
  //        << "    `*cast` VARCHAR(5) DEFAULT NULL,\n"
  //        << "    `bin` VARCHAR(5) DEFAULT NULL,\n"
  //        << "    `map` SMALLINT DEFAULT NULL,\n"
  //        << "    `red` SMALLINT DEFAULT NULL,\n"
  //        << "    `flow#` INT DEFAULT NULL,\n"
  //        << "    `map_loc` VARCHAR(500) DEFAULT NULL,\n"
  //        << "    `red_loc` VARCHAR(500) DEFAULT NULL,\n"
  //        << "    `bn_load_Gbit` DOUBLE DEFAULT NULL,\n"
  //        << "    `lb_optc` DOUBLE DEFAULT NULL,\n"
  //        << "    `lb_elec` DOUBLE DEFAULT NULL,\n"
  //        << "    `lb_elec_bit` BIGINT DEFAULT NULL,\n"
  //        << "    `ttl_Gbit` DOUBLE DEFAULT NULL,\n"
  //        << "    `avg_Gbit` DOUBLE DEFAULT NULL,\n"
  //        << "    `min_Gbit` DOUBLE DEFAULT NULL,\n"
  //        << "    `max_Gbit` DOUBLE DEFAULT NULL,\n"
  //        << "    `tArr` DOUBLE DEFAULT NULL,\n"
  //        << "    `tFin` DOUBLE DEFAULT NULL,\n"
  //        << "    `cct` DOUBLE DEFAULT NULL,\n"
  //        << "    `r_optc` DOUBLE DEFAULT NULL,\n"
  //        << "    `r_elec` DOUBLE DEFAULT NULL,\n"
  //        << "    PRIMARY KEY (`job_id`)\n"
  //        << ");";
  //  cout << "Created table " << table_name << endl;
  has_init_coflow_info_table_ = true;
}

void DbLogger::WriteCoflowFeatures(Coflow *coflow) {
  if (!Connect()) return;
  if (!has_init_coflow_info_table_) InitCoflowInfoTable();
  // read a coflow. start building up knowledge.
  double total_flow_size_Gbit = 0.0;
  double min_flow_size_Gbit = -1.0;
  double max_flow_size_Gbit = -1.0;

  const map<pair<int, int>, long> &mr_demand_byte = coflow->GetMRFlowBytes();

  int flow_num = (int) coflow->GetFlows()->size();
  for (Flow *flow : *(coflow->GetFlows())) {
    double this_flow_size_Gbit = ((double) flow->GetSizeInBit() / 1e9);
    total_flow_size_Gbit += this_flow_size_Gbit; // each flow >= 1MB
    if (min_flow_size_Gbit > this_flow_size_Gbit || min_flow_size_Gbit < 0) {
      min_flow_size_Gbit = this_flow_size_Gbit;
    }
    if (max_flow_size_Gbit < this_flow_size_Gbit || max_flow_size_Gbit < 0) {
      max_flow_size_Gbit = this_flow_size_Gbit;
    }
  }

  double avg_flow_size_Gbit = total_flow_size_Gbit / (double) flow_num;

  string cast_pattern = "";
  int map = coflow->GetNumMap();
  int red = coflow->GetNumRed();

  if (map == 1 && red == 1) {
    cast_pattern = "1";               // single-flow
  } else if (map > 1 && red == 1) {
    cast_pattern = "m21";            // incast
  } else if (map == 1 && red > 1) {
    cast_pattern = "12m";           // one sender, multiple receiver.
  } else if (map > 1 && red > 1) {
    cast_pattern = "m2m";           // many-to-many
  }

  string bin;
  // double lb_elec_MB = lb_elec / (double) 8.0/ 1000000;
  bin += (avg_flow_size_Gbit / 8.0 * 1e3 < 5.0) ?
         'S' : 'L'; // short if avg flow size < 5MB, long other wise
  bin += (flow_num <= 50) ? 'N' : 'W';// narrow if max flow# <= 50

  string table_name = GetTableName("CoflowInfo");
  // sql to write table. You may also write to file named TRAFFIC_AUDIT_FILE_NAME
  // use INSERT and UPDATE so that part of the info written may stay.
  //  query << "INSERT INTO " << table_name << " "
  //        << "(`job_id`, `*cast`, `bin`, `map`, `red`, `flow#`, "
  //        << "`map_loc`, `red_loc`, "
  //        << "`bn_load_Gbit`, `lb_optc`, `lb_elec`, `lb_elec_bit`, "
  //        << "`ttl_Gbit`,  `avg_Gbit`, `min_Gbit`, `max_Gbit`) VALUES ("
  //        << coflow->GetJobId() << ','
  //        << "'" << cast_pattern << "'" << ','
  //        << "'" << bin << "'" << ','
  //        << map << ','
  //        << red << ','
  //        << flow_num << ','
  //        << "'" << Join(coflow->GetMapperLocations(), '_') << "'" << ','
  //        << "'" << Join(coflow->GetReducerLocations(), '_') << "'" << ','
  //        << coflow->GetMaxMapRedLoadGb() << ','             // bn_load_GB
  //        << coflow->GetMaxOptimalWorkSpanInSeconds() << ',' // lb_optc
  //        << coflow->GetMaxPortLoadInSec() << ','            // lb_elec
  //        << coflow->GetMaxPortLoadInBits() << ','           // lb_elec_bit
  //        << total_flow_size_Gbit << ','
  //        << avg_flow_size_Gbit << ','
  //        << min_flow_size_Gbit << ','
  //        << max_flow_size_Gbit << ") "
  //        << "ON DUPLICATE KEY UPDATE "
  //        << "`*cast`= values (`*cast`), "
  //        << "`bin`= values (`bin`), "
  //        << "`map`= values (`map`), "
  //        << "`red`= values (`red`), "
  //        << "`flow#`= values (`flow#`), "
  //        << "`map_loc`= values (`map_loc`), "
  //        << "`red_loc`= values (`red_loc`), "
  //        << "`lb_optc`= values (`lb_optc`), "
  //        << "`lb_elec`= values (`lb_elec`), "
  //        << "`lb_elec_bit`= values (`lb_elec_bit`), "
  //        << "`ttl_Gbit`= values (`ttl_Gbit`), "
  //        << "`avg_Gbit`= values (`avg_Gbit`), "
  //        << "`min_Gbit`= values (`min_Gbit`), "
  //        << "`max_Gbit`= values (`max_Gbit`); ";
}

void DbLogger::WriteOnCoflowFinish(double finish_time,
                                   Coflow *coflow) {
  if (!has_init_coflow_info_table_) InitCoflowInfoTable();
  double cct = (finish_time - coflow->GetStartTime());
  double optc_lb = coflow->GetMaxOptimalWorkSpanInSeconds();
  double elec_lb = coflow->GetMaxPortLoadInSeconds();
  double cct_over_optc = cct / optc_lb;
  double cct_over_elec = cct / elec_lb;

  string table_name = GetTableName("CoflowInfo");
  // sql to write table. You may also write to file named TRAFFIC_AUDIT_FILE_NAME
  // use INSERT and UPDATE so that part of the info written may stay.
  //  query << "INSERT INTO " << table_name << " "
  //        << "(`job_id`, `tArr`, `tFin`, `cct`, `r_optc`, `r_elec`) VALUES ("
  //        << coflow->GetJobId() << ','
  //        << coflow->GetStartTime() << ','
  //        << finish_time << ','
  //        << (cct > 0 ? to_string(cct) : "null") << ','
  //        << (optc_lb > 0 ? to_string(cct_over_optc) : "null") << ','
  //        << (optc_lb > 0 ? to_string(cct_over_elec) : "null") << ") "
  //        << "ON DUPLICATE KEY UPDATE "
  //        << "`tArr`= values (`tArr`), "
  //        << "`tFin`= values (`tFin`), "
  //        << "`cct`= values (`cct`), "
  //        << "`r_optc`= values (`r_optc`), "
  //        << "`r_elec`= values (`r_elec`); ";
}

void DbLogger::InitFlowInfoTable() {
  if (!DropIfExist("FlowInfo")) return;
  string table_name = GetTableName("FlowInfo");
  // table schema
  //  query << "CREATE TABLE `" << table_name << "` (\n"
  //        << "    `job_id` INT(11) NOT NULL,\n"
  //        << "    `flow_id` INT(11) NOT NULL,\n"
  //        << "    `insert_ts` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
  //        << "    `src` smallint NOT NULL,\n"
  //        << "    `dst` smallint NOT NULL,\n"
  //        << "    `flow_size_bit` BIGINT DEFAULT NULL,\n"
  //        << "    `tArr` DOUBLE DEFAULT NULL,\n"
  //        << "    `tFin` DOUBLE DEFAULT NULL,\n"
  //        << "    `fct` DOUBLE DEFAULT NULL,\n"
  //        << "    PRIMARY KEY (`flow_id`)\n"
  //        << ");";
  //  cout << "Created table " << table_name << endl;
}
void DbLogger::WriteOnFlowFinish(double finish_time, Flow *flow) {
  if (!LOG_FLOW_INFO_) return;
  double fct = (finish_time - flow->GetStartTime());

  string table_name = GetTableName("FlowInfo");
  // sql to write table. You may also write to file named TRAFFIC_AUDIT_FILE_NAME
  // always use INSERT as there is appending.
  //  query << "INSERT INTO " << table_name << " "
  //        << "(`job_id`, `flow_id`, `src` ,`dst`, `flow_size_bit`, "
  //        << "`tArr`, `tFin`, `fct`) VALUES ("
  //        << flow->GetParentCoflow()->GetJobId() << ','
  //        << flow->GetFlowId() << ','
  //        << flow->GetSrc() << ','
  //        << flow->GetDest() << ','
  //        << flow->GetSizeInBit() << ','
  //        << flow->GetStartTime() << ','
  //        << finish_time << ','
  //        << fct << ");";
}