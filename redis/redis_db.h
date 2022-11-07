//
//  rocksdb_db.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_REDIS_DB_H_
#define YCSB_C_REDIS_DB_H_

#include <string>
#include <mutex>

#include "core/db.h"
#include "core/properties.h"

#include <sw/redis++/redis++.h>
#include <sw/redis++/redis.h>

namespace ycsbc {

  class RedisDB : public DB {
  public:

    void Init();

    void Cleanup();

    Status Read(const std::string &table, const std::string &key,
                const std::vector<std::string> *fields,
                std::unordered_map<std::string, std::string> &result);

    Status Scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields,
                std::vector<std::unordered_map<std::string, std::string>> &result);

    Status Update(const std::string &table, const std::string &key,
                  std::unordered_map<std::string, std::string> &values);

    Status Insert(const std::string &table, const std::string &key,
                  std::unordered_map<std::string, std::string> &values);

    Status Delete(const std::string &table, const std::string &key);

  private:

    sw::redis::RedisCluster *cluster_ptr;
    int fieldcount_;
    static int ref_cnt_;
    static std::mutex mu_;
    std::string index_name;
    int max_try;
  };

  DB *NewRedisDB();

} // ycsbc

#endif

