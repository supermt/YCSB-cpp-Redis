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
  const std::string REDIS_PORT = "redis.port";
  const std::string REDIS_PORT_DEFAULT = "30001";

  const std::string REDIS_HOST = "redis.host";
  const std::string REDIS_HOST_DEFAULT = "127.0.0.1";

  const std::string REDIS_INDEX_NAME = "redis.index";
  const std::string REDIS_INDEX_NAME_DEFAULT = "_indices";

} // anonymous

namespace ycsbc {

// Create Redis link
  using namespace sw::redis;
  const bool registered = DBFactory::RegisterDB("redis", NewRedisDB);

  DB *NewRedisDB() {
   return new RedisDB;
  }

  void RedisDB::Init() {
   auto host = props_->GetProperty(REDIS_HOST, REDIS_HOST_DEFAULT);
   auto port = props_->GetProperty(REDIS_PORT, REDIS_PORT_DEFAULT);

   std::string target_link_posi = "tcp://" + host + ":" + port;
   std::cout << "Initializing, create redis cluster link at "
             << target_link_posi << std::endl;

   cluster_ptr = new RedisCluster(target_link_posi);
   std::cout
       << ">>>>>>>>>>>>>>>>>>>>>>>>>>> Connected! <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
       << std::endl;

   ycsb_command_ptr = new Redis(cluster_ptr->redis("ycsb_link"));;

   auto reply = ycsb_command_ptr->command("stats");
   assert(reply);
   auto val = reply::parse<OptionalString>(*reply);
   if (val) {
    std::cout << *val << std::endl;
   }
   return;
  }

  void RedisDB::Cleanup() {

   auto reply = ycsb_command_ptr->command("stats");
   assert(reply);
   auto val = reply::parse<OptionalString>(*reply);
   if (val) {
    std::cout << *val << std::endl;
   }
   delete ycsb_command_ptr;
   delete cluster_ptr;
   std::cout
       << ">>>>>>>>>>>>>>>>>>>>>>>>>>> Clean the DB, close the redis cluster <<<<<<<<<<<<<<<<<<<<<<<<<<<"
       << std::endl;

   return;
  }

  DB::Status RedisDB::Read(const std::string &table, const std::string &key,
                           const std::vector<std::string> *fields,
                           std::unordered_map<std::string, std::string> &result) {
   return DB::kNotFound;
  }

  DB::Status
  RedisDB::Scan(const std::string &table, const std::string &key, int len,
                const std::vector<std::string> *fields,
                std::vector<std::unordered_map<std::string, std::string>> &result) {
   return DB::kNotFound;
  }

  DB::Status RedisDB::Update(const std::string &table, const std::string &key,
                             std::unordered_map<std::string, std::string> &values) {
   return DB::kNotFound;
  }

  DB::Status RedisDB::Insert(const std::string &table, const std::string &key,
                             std::unordered_map<std::string, std::string> &values) {
   std::unordered_map<std::string, std::string> input_stream;
   ycsb_command_ptr->hmset(key, input_stream.begin(), input_stream.end());

   return DB::kNotFound;
  }

  DB::Status RedisDB::Delete(const std::string &table, const std::string &key) {
   return DB::kNotFound;
  }

} // ycsbc
