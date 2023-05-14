//
//  utils.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/5/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_UTILS_H_
#define YCSB_C_UTILS_H_

#include <algorithm>
#include <cstdint>
#include <exception>
#include <random>

namespace ycsbc {

    namespace utils {

        const uint64_t kFNVOffsetBasis64 = 0xCBF29CE484222325ull;
        const uint64_t kFNVPrime64 = 1099511628211ull;

        inline uint64_t FNVHash64(uint64_t
                                  val) {
            uint64_t hash = kFNVOffsetBasis64;

            for (
                    int i = 0;
                    i < 8; i++) {
                uint64_t octet = val & 0x00ff;
                val = val >> 8;

                hash = hash ^ octet;
                hash = hash * kFNVPrime64;
            }
            return
                    hash;
        }

        inline std::vector<std::string> split(std::string s, std::string delimiter) {
            size_t pos_start = 0, pos_end, delim_len = delimiter.length();
            std::string token;
            std::vector<std::string> res;

            while ((pos_end = s.find(delimiter, pos_start)) != std::string::npos) {
                token = s.substr(pos_start, pos_end - pos_start);
                pos_start = pos_end + delim_len;
                res.push_back(token);
            }

            res.push_back(s.substr(pos_start));
            return res;
        }

        inline uint64_t Hash(uint64_t val) { return FNVHash64(val); }

        inline uint32_t ThreadLocalRandomInt() {
            static thread_local std::random_device rd;
            static thread_local std::minstd_rand rn(rd());
            return rn();
        }

        inline double ThreadLocalRandomDouble(double min = 0.0, double max = 1.0) {
            static thread_local std::random_device rd;
            static thread_local std::minstd_rand rn(rd());
            static thread_local std::uniform_real_distribution<double> uniform(min, max);
            return uniform(rn);
        }

///
/// Returns an ASCII code that can be printed to desplay
///
        inline char RandomPrintChar() {
            return rand() % 94 + 33;
        }

        class Random64 {
        private:
            std::mt19937_64 generator_;

        public:
            explicit Random64(uint64_t s) : generator_(s) {}

// Generates the next random number
            uint64_t Next() { return generator_(); }

// Returns a uniformly distributed value in the range [0..n-1]
// REQUIRES: n > 0
            uint64_t Uniform(uint64_t n) {
                return std::uniform_int_distribution<uint64_t>(0, n - 1)(generator_);
            }

// Randomly returns true ~"1/n" of the time, and false otherwise.
// REQUIRES: n > 0
            bool OneIn(uint64_t n) { return Uniform(n) == 0; }

// Skewed: pick "base" uniformly from range [0,max_log] and then
// return "base" random bits.  The effect is to pick a number in the
// range [0,2^max_log-1] with exponential bias towards smaller numbers.
            uint64_t Skewed(int max_log) {
                return Uniform(uint64_t(1) << Uniform(max_log + 1));
            }
        };

        class Exception : public std::exception {
        public:
            Exception(const std::string &message) : message_(message) {}

            const char *what() const noexcept {
                return message_.c_str();
            }

        private:
            std::string message_;
        };

        inline bool StrToBool(std::string str) {
            std::transform(str.begin(), str.end(), str.begin(), ::tolower);
            if (str == "true" || str == "1") {
                return true;
            } else if (str == "false" || str == "0") {
                return false;
            } else {
                throw Exception("Invalid bool string: " + str);
            }
        }

        inline std::string Trim(const std::string &str) {
            auto front = std::find_if_not(str.begin(), str.end(), [](int c) { return std::isspace(c); });
            return std::string(front, std::find_if_not(str.rbegin(), std::string::const_reverse_iterator(front),
                                                       [](int c) { return std::isspace(c); }).base());
        }

    } // utils

} // ycsbc

#endif // YCSB_C_UTILS_H_
