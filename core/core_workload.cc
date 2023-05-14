//
//  core_workload.cc
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "utils.h"
#include "uniform_generator.h"
#include "zipfian_generator.h"
#include "scrambled_zipfian_generator.h"
#include "skewed_latest_generator.h"
#include "const_generator.h"
#include "core_workload.h"
#include "random_byte_generator.h"

#include <algorithm>
#include <iostream>
#include <random>
#include <string>
#include <regex>

using ycsbc::CoreWorkload;
using std::string;

const char *ycsbc::kOperationString[ycsbc::MAXOPTYPE] = {
        "INSERT",
        "READ",
        "UPDATE",
        "SCAN",
        "READMODIFYWRITE",
        "DELETE"
};

const string CoreWorkload::TABLENAME_PROPERTY = "table";
const string CoreWorkload::TABLENAME_DEFAULT = "usertable";

const string CoreWorkload::FIELD_COUNT_PROPERTY = "fieldcount";
const string CoreWorkload::FIELD_COUNT_DEFAULT = "10";

const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_PROPERTY = "field_len_dist";
const string CoreWorkload::FIELD_LENGTH_DISTRIBUTION_DEFAULT = "constant";

const string CoreWorkload::FIELD_LENGTH_PROPERTY = "fieldlength";
const string CoreWorkload::FIELD_LENGTH_DEFAULT = "100";

const string CoreWorkload::READ_ALL_FIELDS_PROPERTY = "readallfields";
const string CoreWorkload::READ_ALL_FIELDS_DEFAULT = "true";

const string CoreWorkload::WRITE_ALL_FIELDS_PROPERTY = "writeallfields";
const string CoreWorkload::WRITE_ALL_FIELDS_DEFAULT = "false";

const string CoreWorkload::READ_PROPORTION_PROPERTY = "readproportion";
const string CoreWorkload::READ_PROPORTION_DEFAULT = "0.95";

const string CoreWorkload::UPDATE_PROPORTION_PROPERTY = "updateproportion";
const string CoreWorkload::UPDATE_PROPORTION_DEFAULT = "0.05";

const string CoreWorkload::INSERT_PROPORTION_PROPERTY = "insertproportion";
const string CoreWorkload::INSERT_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::SCAN_PROPORTION_PROPERTY = "scanproportion";
const string CoreWorkload::SCAN_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";
const string CoreWorkload::READMODIFYWRITE_PROPORTION_DEFAULT = "0.0";

const string CoreWorkload::REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
const string CoreWorkload::REQUEST_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::TOTAL_SLOTS_PROPERTY = "slot_no";
const string CoreWorkload::TOTAL_SLOTS_DEFAULT = "16384";

const string CoreWorkload::KEY_RANGE_NO_PROPERTY = "keyrange_no";
const string CoreWorkload::KEY_RANGE_NO_DEFAULT = "0"; // 30 for the default value in mixgraph

const string CoreWorkload::KEY_RANGE_DIST_A_PROPERTY = "keyrange_dist_a";
const string CoreWorkload::KEY_RANGE_DIST_A_DEFAULT = "14.18";
const string CoreWorkload::KEY_RANGE_DIST_B_PROPERTY = "keyrange_dist_b";
const string CoreWorkload::KEY_RANGE_DIST_B_DEFAULT = "-2.917";
const string CoreWorkload::KEY_RANGE_DIST_C_PROPERTY = "keyrange_dist_c";
const string CoreWorkload::KEY_RANGE_DIST_C_DEFAULT = "0.0164";
const string CoreWorkload::KEY_RANGE_DIST_D_PROPERTY = "keyrange_dist_d";
const string CoreWorkload::KEY_RANGE_DIST_D_DEFAULT = "-0.08082";

const string CoreWorkload::KEY_RANGE_HOTNESS_ARRAY_PROPERTY = "hotness_array";
const string CoreWorkload::KEY_RANGE_HOTNESS_ARRAY_DEFAULT = "0.7,0.3";

const string CoreWorkload::ZERO_PADDING_PROPERTY = "zeropadding";
const string CoreWorkload::ZERO_PADDING_DEFAULT = "1";

const string CoreWorkload::KEY_LEN_PROPERTY = "keylen";
const string CoreWorkload::KEY_LEN_DEFAULT = "16";

const string CoreWorkload::MIN_SCAN_LENGTH_PROPERTY = "minscanlength";
const string CoreWorkload::MIN_SCAN_LENGTH_DEFAULT = "1";

const string CoreWorkload::MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";
const string CoreWorkload::MAX_SCAN_LENGTH_DEFAULT = "1000";

const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";
const string CoreWorkload::SCAN_LENGTH_DISTRIBUTION_DEFAULT = "uniform";

const string CoreWorkload::INSERT_ORDER_PROPERTY = "insertorder";
const string CoreWorkload::INSERT_ORDER_DEFAULT = "hashed";

const string CoreWorkload::INSERT_START_PROPERTY = "insertstart";
const string CoreWorkload::INSERT_START_DEFAULT = "0";

const string CoreWorkload::RECORD_COUNT_PROPERTY = "recordcount";
const string CoreWorkload::OPERATION_COUNT_PROPERTY = "operationcount";

const std::string CoreWorkload::FIELD_NAME_PREFIX = "fieldnameprefix";
const std::string CoreWorkload::FIELD_NAME_PREFIX_DEFAULT = "field";

void CoreWorkload::Init(const utils::Properties &p) {
    table_name_ = p.GetProperty(TABLENAME_PROPERTY, TABLENAME_DEFAULT);

    field_count_ = std::stoi(
            p.GetProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_DEFAULT));
    field_prefix_ = p.GetProperty(FIELD_NAME_PREFIX, FIELD_NAME_PREFIX_DEFAULT);
    field_len_generator_ = GetFieldLenGenerator(p);

    double read_proportion = std::stod(p.GetProperty(READ_PROPORTION_PROPERTY,
                                                     READ_PROPORTION_DEFAULT));
    double update_proportion = std::stod(p.GetProperty(UPDATE_PROPORTION_PROPERTY,
                                                       UPDATE_PROPORTION_DEFAULT));
    double insert_proportion = std::stod(p.GetProperty(INSERT_PROPORTION_PROPERTY,
                                                       INSERT_PROPORTION_DEFAULT));
    double scan_proportion = std::stod(p.GetProperty(SCAN_PROPORTION_PROPERTY,
                                                     SCAN_PROPORTION_DEFAULT));
    double readmodifywrite_proportion = std::stod(p.GetProperty(
            READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_DEFAULT));

    record_count_ = std::stoi(p.GetProperty(RECORD_COUNT_PROPERTY));
    std::string request_dist = p.GetProperty(REQUEST_DISTRIBUTION_PROPERTY,
                                             REQUEST_DISTRIBUTION_DEFAULT);
    key_range_count_ = std::stoi(p.GetProperty(KEY_RANGE_NO_PROPERTY, KEY_RANGE_NO_DEFAULT));
    total_slot_number_ = std::stoi(p.GetProperty(TOTAL_SLOTS_PROPERTY, TOTAL_SLOTS_DEFAULT));

    if (key_range_count_ > 1) {
        auto temp = p.GetProperty(KEY_RANGE_HOTNESS_ARRAY_PROPERTY, KEY_RANGE_HOTNESS_ARRAY_DEFAULT);
        auto temp_vector = utils::split(temp, ",");
        int i = 0;
        for (const auto &num_str: temp_vector) {
            double ratio = std::stod(num_str);
            key_range_ratio_.push_back(ratio);
            slot_chooser_.AddValue(i, ratio);
            i++;
        }
    }

    int min_scan_len = std::stoi(
            p.GetProperty(MIN_SCAN_LENGTH_PROPERTY, MIN_SCAN_LENGTH_DEFAULT));
    int max_scan_len = std::stoi(
            p.GetProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_DEFAULT));
    std::string scan_len_dist = p.GetProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY,
                                              SCAN_LENGTH_DISTRIBUTION_DEFAULT);
    int insert_start = std::stoi(
            p.GetProperty(INSERT_START_PROPERTY, INSERT_START_DEFAULT));

    zero_padding_ = std::stoi(
            p.GetProperty(ZERO_PADDING_PROPERTY, ZERO_PADDING_DEFAULT));

    key_len_ = std::stoi(
            p.GetProperty(KEY_LEN_PROPERTY, KEY_LEN_DEFAULT));

    read_all_fields_ = utils::StrToBool(p.GetProperty(READ_ALL_FIELDS_PROPERTY,
                                                      READ_ALL_FIELDS_DEFAULT));
    write_all_fields_ = utils::StrToBool(p.GetProperty(WRITE_ALL_FIELDS_PROPERTY,
                                                       WRITE_ALL_FIELDS_DEFAULT));

    if (p.GetProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_DEFAULT) == "hashed") {
        ordered_inserts_ = false;
    } else {
        ordered_inserts_ = true;
    }


    if (read_proportion > 0) {
        op_chooser_.AddValue(READ, read_proportion);
    }
    if (update_proportion > 0) {
        op_chooser_.AddValue(UPDATE, update_proportion);
    }
    if (insert_proportion > 0) {
        op_chooser_.AddValue(INSERT, insert_proportion);
    }
    if (scan_proportion > 0) {
        op_chooser_.AddValue(SCAN, scan_proportion);
    }
    if (readmodifywrite_proportion > 0) {
        op_chooser_.AddValue(READMODIFYWRITE, readmodifywrite_proportion);
    }

    insert_key_sequence_ = new CounterGenerator(insert_start);
    transaction_insert_key_sequence_ = new AcknowledgedCounterGenerator(
            record_count_);

    if (request_dist == "uniform") {
        key_chooser_ = new UniformGenerator(0, record_count_ - 1);
    } else if (request_dist == "zipfian") {
        // If the number of keys changes, we don't want to change popular keys.
        // So we construct the scrambled zipfian generator with a keyspace
        // that is larger than what exists at the beginning of the test.
        // If the generator picks a key that is not inserted yet, we just ignore it
        // and pick another key.
        int op_count = std::stoi(p.GetProperty(OPERATION_COUNT_PROPERTY));
        int new_keys = (int) (op_count * insert_proportion * 2); // a fudge factor
        key_chooser_ = new ScrambledZipfianGenerator(record_count_ + new_keys);

    } else if (request_dist == "latest") {
        key_chooser_ = new SkewedLatestGenerator(*transaction_insert_key_sequence_);
    } else if (request_dist == "mixgraph") {
        double a = std::stod(p.GetProperty(KEY_RANGE_DIST_A_PROPERTY, KEY_RANGE_DIST_A_DEFAULT));
        double b = std::stod(p.GetProperty(KEY_RANGE_DIST_B_PROPERTY, KEY_RANGE_DIST_B_DEFAULT));
        double c = std::stod(p.GetProperty(KEY_RANGE_DIST_C_PROPERTY, KEY_RANGE_DIST_C_DEFAULT));
        double d = std::stod(p.GetProperty(KEY_RANGE_DIST_D_PROPERTY, KEY_RANGE_DIST_D_DEFAULT));
        key_chooser_ = new TwoTermExpKeysGenerator(key_range_count_, record_count_, a, b, c, d);
//        key_range_result = fopen("prefix_list.txt", "w");
    } else {
        throw utils::Exception("Unknown request distribution: " + request_dist);
    }

    field_chooser_ = new UniformGenerator(0, field_count_ - 1);

    if (scan_len_dist == "uniform") {
        scan_len_chooser_ = new UniformGenerator(min_scan_len, max_scan_len);
    } else if (scan_len_dist == "zipfian") {
        scan_len_chooser_ = new ZipfianGenerator(min_scan_len, max_scan_len);
    } else {
        throw utils::Exception(
                "Distribution not allowed for scan length: " + scan_len_dist);
    }
}

ycsbc::Generator<uint64_t> *CoreWorkload::GetFieldLenGenerator(
        const utils::Properties &p) {
    string field_len_dist = p.GetProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY,
                                          FIELD_LENGTH_DISTRIBUTION_DEFAULT);
    int field_len = std::stoi(
            p.GetProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_DEFAULT));
    std::cout << "filed length is: " << field_len << std::endl;
    if (field_len_dist == "constant") {
        return new ConstGenerator(field_len);
    } else if (field_len_dist == "uniform") {
        return new UniformGenerator(1, field_len);
    } else if (field_len_dist == "zipfian") {
        return new ZipfianGenerator(1, field_len);
    } else {
        throw utils::Exception(
                "Unknown field length distribution: " + field_len_dist);
    }
}

std::string CoreWorkload::BuildKeyName(uint64_t key_num) {
    if (!ordered_inserts_) {
        key_num = utils::Hash(key_num);
    }
    std::string prekey;
    if (key_range_count_ > 1) {
        prekey = "{" + std::to_string(key_chooser_->LastPrefix()) + "}";
        key_num = key_chooser_->Last() - key_chooser_->LastPrefix();
    } else {
        prekey = "user";
    }
    std::string value = std::to_string(key_num);
    int fill = 0;
    if (key_range_count_ > 1) {
        fill = std::max(0, key_len_ - static_cast<int>(value.size() + prekey.size()));
    } else {
        fill = std::max(0, zero_padding_ - static_cast<int>(value.size()));
    }
    auto result = prekey.append(fill, '0').append(value);
//    fwrite((result + "\n").c_str(), sizeof(char), result.size() + 1, key_range_result);
    return prekey.append(fill, '0').append(value);
}

void CoreWorkload::BuildValues(
        std::unordered_map<std::string, std::string> &values) {
    for (int i = 0; i < field_count_; ++i) {
        std::string name;
        name.append(field_prefix_).append(std::to_string(i));
        uint64_t len = field_len_generator_->Next();
        std::string value;
        value.reserve(len);
        RandomByteGenerator byte_generator;
        std::generate_n(std::back_inserter(value), len,
                        [&]() { return byte_generator.Next(); });
        values.emplace(name, value);
    }
}

void CoreWorkload::BuildSingleValue(
        std::unordered_map<std::string, std::string> &values) {

    std::string name, value;
    name.append(NextFieldName());
    uint64_t len = field_len_generator_->Next();
    value.reserve(len);
    RandomByteGenerator byte_generator;
    std::generate_n(std::back_inserter(value), len,
                    [&]() { return byte_generator.Next(); });
    values.emplace(name, value);
}

