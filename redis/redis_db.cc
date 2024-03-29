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

namespace {
    const std::string REDIS_PORT = "redis.port";
    const std::string REDIS_PORT_DEFAULT = "30001";

    const std::string REDIS_HOST = "redis.host";
    const std::string REDIS_HOST_DEFAULT = "127.0.0.1";

    const std::string REDIS_INDEX_NAME = "redis.index";
    const std::string REDIS_INDEX_NAME_DEFAULT = "_indices";

    const std::string REDIS_MAX_TRY_NAME = "redis.max_try";
    const std::string REDIS_MAX_TRY_DEFAULT = "1000";

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

        index_name = props_->GetProperty(REDIS_INDEX_NAME, REDIS_INDEX_NAME_DEFAULT);
        max_try = std::stoi(
                props_->GetProperty(REDIS_MAX_TRY_NAME, REDIS_MAX_TRY_DEFAULT));

        std::string target_link_posi = "tcp://" + host + ":" + port;
        std::cout << "Initializing, create redis cluster link at "
                  << target_link_posi << std::endl;


        cluster_ptr = new RedisCluster(target_link_posi);
        assert(cluster_ptr);
//        std::unordered_map<std::string, std::string> str_map = {{"f1", "v1"},
//                                                                {"f2", "v2"},
//                                                               {"f3", "v3"}};
//        try { cluster_ptr->hmset("test", str_map.begin(), str_map.end()); }
//        catch (const Error &e) {
//            std::cout << "Testing failed" << e.what() << std::endl;
//        }
//
//        std::cout << ">>>>>>>>>>>>>>>>>>>>>>>>>>> Connected! <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
//                  << std::endl;
    }

    void RedisDB::Cleanup() {
        delete cluster_ptr;
        std::cout
                << ">>>>>>>>>>>>>>>>>>>>>>>>>>> Clean the DB, close the redis cluster <<<<<<<<<<<<<<<<<<<<<<<<<<<"
                << std::endl;
    }

    DB::Status RedisDB::Read(const std::string &table, const std::string &key,
                             const std::vector<std::string> *fields,
                             std::unordered_map<std::string, std::string> &result) {
        bool success = false;
        int try_times = 0;
        try {
            while (!success) {
                if (fields == nullptr) {
                    cluster_ptr->hgetall(key, std::inserter(result, result.begin()));
                } else {
                    std::vector<OptionalString> vals;
                    cluster_ptr->hmget(key, fields->begin(), fields->end(),
                                       std::back_inserter(vals));

                    for (uint32_t i = 0; i < fields->size(); i++) {
                        result.emplace(fields->at(i), vals.at(i).value());
                    }
                }
                try_times++;
                success = true;
            }
        } catch (const IoError &link_error) {
            assert(false);
        }
        catch (const MovedError &e) {
            assert(false);
        } catch (const Error &e) {
            return DB::kError;
        }
        return result.empty() ? kNotFound : kOK;
    }

    inline std::wstring s2ws(const std::string &str) {
        if (str.empty()) {
            return L"";
        }
        unsigned len = str.size() + 1;
        setlocale(LC_CTYPE, "en_US.UTF-8");
        auto *p = new wchar_t[len];
        mbstowcs(p, str.c_str(), len);
        std::wstring w_str(p);
        delete[] p;
        return w_str;
    }

    inline int32_t hash(const std::string &input) {
        int32_t h = 0;
        std::wstring wstr = s2ws(input);
        if (wstr.length() > 0) {
            for (wchar_t i: wstr) {
                h = 31 * h + i;
            }
        }
        return h;
    }


    DB::Status RedisDB::Update(const std::string &table, const std::string &key,
                               std::unordered_map<std::string, std::string> &values) {

        bool success = false;
        int try_times = 0;
        try {
            while (!success) {
                cluster_ptr->hmset(key, values.begin(),
                                   values.end());
                if (try_times > 1) {
                    std::cout << "tried for :" << try_times << "times" << std::endl;
                }
                try_times++;
                success = true;
            }
            return DB::kOK;
        } catch (const IoError &link_error) {
            assert(false);
        }
        catch (const MovedError &e) {
            assert(false);
        } catch (const Error &e) {
            return DB::kError;
        }
        return DB::kError;
    }

    DB::Status RedisDB::Insert(const std::string &table, const std::string &key,
                               std::unordered_map<std::string, std::string> &values) {
        try {
            cluster_ptr->hmset(key, values.begin(),
                               values.end());
        } catch (const IoError &link_error) {
            assert(false);
        }
        catch (const MovedError &e) {
            assert(false);
        } catch (const Error &e) {
            return DB::kError;
        }
        return DB::kOK;
    }

    DB::Status RedisDB::Delete(const std::string &table, const std::string &key) {
        try {
            if (cluster_ptr->del(key) == 0 &&
                cluster_ptr->zrem(index_name, key) == 0) {
                return DB::kNotFound;
            } else {
                return DB::kOK;
            }
        } catch (const IoError &link_error) {
            assert(false);
        }
        catch (const MovedError &e) {
            assert(false);
        } catch (const Error &e) {
            return DB::kError;
        }
        return DB::kOK;
    }

    DB::Status
    RedisDB::Scan(const std::string &table, const std::string &start_key, int len,
                  const std::vector<std::string> *fields,
                  std::vector<std::unordered_map<std::string, std::string>> &result) {
        std::vector<std::pair<std::string, double>> zset_result;
        try {

            cluster_ptr->zrangebyscore(index_name,
                                       LeftBoundedInterval<double>(hash(start_key),
                                                                   BoundType::RIGHT_OPEN),
                                       {0, len},
                                       std::back_inserter(zset_result));
            for (auto key: zset_result) {
                std::unordered_map<std::string, std::string> values;
                Read(table, key.first, fields, values);
                result.push_back(values);
            }
        } catch (const IoError &link_error) {
            assert(false);
        }
        catch (const MovedError &e) {
            assert(false);
        } catch (const Error &e) {
            return DB::kError;
        }
        return kOK;
    }

} // ycsbc
