//
//  rocksdb_db.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#include <iostream>
#include "redis_db.h"

#include "core/core_workload.h"
#include "core/db_factory.h"
#include "core/properties.h"
#include "core/utils.h"

namespace {
    const std::string REDIS_PORT = "reids.port";
    const std::string REDIS_PORT_DEFAULT = "30001";

    const std::string REDIS_HOST = "redis.host";
    const std::string REDIS_HOST_DEFAULT = "127.0.0.1";
} // anonymous

namespace ycsbc {

// Create Redis link
    using namespace sw::redis;
    const bool registered = DBFactory::RegisterDB("redis", NewRedisDB);

    DB *NewRedisDB() {

        return new RedisDB;
    }

    void RedisDB::Init() {
        std::cout << "Initializing, create redis cluster link" << std::endl;
        auto host = props_->GetProperty(REDIS_HOST, REDIS_HOST_DEFAULT);
        auto port = props_->GetProperty(REDIS_PORT, REDIS_PORT_DEFAULT);
        cluster_ptr = new RedisCluster("tcp://" + host + ":" + port);
        std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>> Connected! <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;
        return;
    }

    void RedisDB::Cleanup() {
        std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>> Clean the DB, close the redis cluster <<<<<<<<<<<<<<<<<<<<<<<<<<<" << std::endl;

        return;
    }

    DB::Status RedisDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                             std::vector<Field> &result) {
        return DB::kNotFound;
    }

    DB::Status
    RedisDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                  std::vector<std::vector<Field>> &result) {
        return DB::kNotFound;
    }

    DB::Status RedisDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
        return DB::kNotFound;
    }

    DB::Status RedisDB::Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
        return DB::kNotFound;
    }

    DB::Status RedisDB::Delete(const std::string &table, const std::string &key) {
        return DB::kNotFound;
    }
} // ycsbc
