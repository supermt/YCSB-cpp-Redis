//
//  measurements.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include "measurements.h"

#include <atomic>
#include <limits>
#include <numeric>
#include <sstream>
#include <iostream>
#include <hdr/hdr_histogram.h>
#include <hdr/hdr_histogram_log.h>
#include <hdr/hdr_interval_recorder.h>

namespace ycsbc {

  Measurements::Measurements() : count_{}, latency_sum_{}, latency_max_{} {
   std::fill(std::begin(latency_min_), std::end(latency_min_),
             std::numeric_limits<uint64_t>::max());
   for (int i = 0; i < MAXOPTYPE; i++) {
    hdr_init(1, INT64_C(3600000000), 3, &this->histogram[i]);
   }

  }

  void Measurements::Report(Operation op, uint64_t latency) {
   count_[op].fetch_add(1, std::memory_order_relaxed);
   latency_sum_[op].fetch_add(latency, std::memory_order_relaxed);
   uint64_t prev_min = latency_min_[op].load(std::memory_order_relaxed);
   while (prev_min > latency
          && !latency_min_[op].compare_exchange_weak(prev_min, latency,
                                                     std::memory_order_relaxed));
   uint64_t prev_max = latency_max_[op].load(std::memory_order_relaxed);
   while (prev_max < latency
          && !latency_max_[op].compare_exchange_weak(prev_max, latency,
                                                     std::memory_order_relaxed));

   hdr_record_value(histogram[op], latency);

  }

  std::string Measurements::GetStatusMsg() {
   std::ostringstream msg_stream;
   msg_stream.precision(2);
   uint64_t total_cnt = 0;
   msg_stream << std::fixed << " operations;";
   for (int i = 0; i < MAXOPTYPE; i++) {
    Operation op = static_cast<Operation>(i);
    uint64_t cnt = GetCount(op);
    if (cnt == 0)
     continue;
    msg_stream << " [" << kOperationString[op] << ":"
               << " Count=" << cnt
               << " Max="
               << latency_max_[op].load(std::memory_order_relaxed) / 1000.0
               << " Min="
               << latency_min_[op].load(std::memory_order_relaxed) / 1000.0
               << " Avg="
               << ((cnt > 0)
                   ? static_cast<double>(latency_sum_[op].load(
                       std::memory_order_relaxed)) / cnt
                   : 0) / 1000.0
               << "]";
    total_cnt += cnt;
   }
   return std::to_string(total_cnt) + msg_stream.str();
  }

  void Measurements::Reset() {
   std::fill(std::begin(count_), std::end(count_), 0);
   std::fill(std::begin(latency_sum_), std::end(latency_sum_), 0);
   std::fill(std::begin(latency_min_), std::end(latency_min_),
             std::numeric_limits<uint64_t>::max());
   std::fill(std::begin(latency_max_), std::end(latency_max_), 0);
  }

  void Measurements::print_hdr_hist(const std::string &prefix) {
   for (int i = 0; i < MAXOPTYPE; i++) {
    Operation op = static_cast<Operation>(i);
    uint64_t cnt = GetCount(op);
    if (cnt != 0) {
     std::string file_name = prefix + kOperationString[op];
     report_files[i] = fopen(file_name.c_str(), "w");
//     fwrite(header.c_str(), sizeof(char), header.size() + 1, report_files[i]);
     hdr_percentiles_print(histogram[op], report_files[i], 5, 1000.0, CSV);
     fflush(report_files[i]);
     fclose(report_files[i]);
    }

   }
  }

} // ycsbc
