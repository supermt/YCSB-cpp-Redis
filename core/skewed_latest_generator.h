//
//  skewed_latest_generator.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/9/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_SKEWED_LATEST_GENERATOR_H_
#define YCSB_C_SKEWED_LATEST_GENERATOR_H_

#include "generator.h"

#include <atomic>
#include <cstdint>
#include "counter_generator.h"
#include "zipfian_generator.h"

namespace ycsbc {

    class TwoTermExpKeysGenerator : public Generator<uint64_t> {
        struct KeyrangeUnit {
            int64_t keyrange_start;
            int64_t keyrange_access;
            int64_t keyrange_keys;
        };

    public:

        // Avoid uninitialized warning-as-error in some compilers
        int64_t keyrange_rand_max_ = 0;
        int64_t keyrange_size_ = 0;
        int64_t keyrange_num_ = 0;
        int64_t last_ = 0;
        int64_t last_prefix_ = 0;
        int FLAGS_num_;

        double key_dist_a_;
        double key_dist_b_;
        double keyrange_dist_a_;
        double keyrange_dist_b_;
        double keyrange_dist_c_;
        double keyrange_dist_d_;

        double read_random_exp_range_;// skewed reading parameter
        utils::Random64 ini_rander;

        std::vector<KeyrangeUnit> keyrange_set_;

        // Initiate the KeyrangeUnit vector and calculate the size of each
        // KeyrangeUnit.
        explicit TwoTermExpKeysGenerator(int keyrange_num, int64_t total_keys, double prefix_a,
                                         double prefix_b, double prefix_c,
                                         double prefix_d) : FLAGS_num_(total_keys), keyrange_dist_a_(prefix_a),
                                                            keyrange_dist_b_(prefix_b),
                                                            keyrange_dist_c_(prefix_c), keyrange_dist_d_(prefix_d),
                                                            read_random_exp_range_(0), ini_rander(0) {
            int64_t amplify = 0;
            int64_t keyrange_start = 0;
            if (keyrange_num <= 0) {
                keyrange_num_ = 1;
            } else {
                keyrange_num_ = keyrange_num;
            }
            keyrange_size_ = total_keys / keyrange_num_;

            // Calculate the key-range shares size based on the input parameters
            for (int64_t pfx = keyrange_num_; pfx >= 1; pfx--) {
                // Step 1. Calculate the probability that this key range will be
                // accessed in a query. It is based on the two-term expoential
                // distribution
                double keyrange_p = prefix_a * std::exp(prefix_b * pfx) +
                                    prefix_c * std::exp(prefix_d * pfx);
                if (keyrange_p < std::pow(10.0, -16.0)) {
                    keyrange_p = 0.0;
                }
                // Step 2. Calculate the amplify
                // In order to allocate a query to a key-range based on the random
                // number generated for this query, we need to extend the probability
                // of each key range from [0,1] to [0, amplify]. Amplify is calculated
                // by 1/(smallest key-range probability). In this way, we ensure that
                // all key-ranges are assigned with an Integer that  >=0
                if (amplify == 0 && keyrange_p > 0) {
                    amplify = static_cast<int64_t>(std::floor(1 / keyrange_p)) + 1;
                }

                // Step 3. For each key-range, we calculate its position in the
                // [0, amplify] range, including the start, the size (keyrange_access)
                KeyrangeUnit p_unit;
                p_unit.keyrange_start = keyrange_start;
                if (0.0 >= keyrange_p) {
                    p_unit.keyrange_access = 0;
                } else {
                    p_unit.keyrange_access =
                            static_cast<int64_t>(std::floor(amplify * keyrange_p));
                }
                p_unit.keyrange_keys = keyrange_size_;
                keyrange_set_.push_back(p_unit);
                keyrange_start += p_unit.keyrange_access;
            }
            keyrange_rand_max_ = keyrange_start;

            // Step 4. Shuffle the key-ranges randomly
            // Since the access probability is calculated from small to large,
            // If we do not re-allocate them, hot key-ranges are always at the end
            // and cold key-ranges are at the begin of the key space. Therefore, the
            // key-ranges are shuffled and the rand seed is only decide by the
            // key-range hotness distribution. With the same distribution parameters
            // the shuffle results are the same.
            utils::Random64 rand_loca(keyrange_rand_max_);
            for (int64_t i = 0; i < keyrange_num; i++) {
                int64_t pos = rand_loca.Next() % keyrange_num;
                assert(i >= 0 && i < static_cast<int64_t>(keyrange_set_.size()) &&
                       pos >= 0 && pos < static_cast<int64_t>(keyrange_set_.size()));
                std::swap(keyrange_set_[i], keyrange_set_[pos]);
            }

            // Step 5. Recalculate the prefix start postion after shuffling
            int64_t offset = 0;
            for (auto &p_unit: keyrange_set_) {
                p_unit.keyrange_start = offset;
                offset += p_unit.keyrange_access;
            }

        }

        // Generate the Key ID according to the input ini_rand and key distribution
        int64_t DistGetKeyID(int64_t ini_rand, double key_dist_a,
                             double key_dist_b) {
            int64_t keyrange_rand = ini_rand % keyrange_rand_max_;

            // Calculate and select one key-range that contains the new key
            int64_t start = 0, end = static_cast<int64_t>(keyrange_set_.size());
            while (start + 1 < end) {
                int64_t mid = start + (end - start) / 2;
                assert(mid >= 0 && mid < static_cast<int64_t>(keyrange_set_.size()));
                if (keyrange_rand < keyrange_set_[mid].keyrange_start) {
                    end = mid;
                } else {
                    start = mid;
                }
            }
            int64_t keyrange_id = start;

            // Select one key in the key-range and compose the keyID
            int64_t key_offset = 0, key_seed;
            if (key_dist_a == 0.0 || key_dist_b == 0.0) {
                key_offset = ini_rand % keyrange_size_;
            } else {
                double u =
                        static_cast<double>(ini_rand % keyrange_size_) / keyrange_size_;
                key_seed = static_cast<int64_t>(
                        ceil(std::pow((u / key_dist_a), (1 / key_dist_b))));
                utils::Random64 rand_key(key_seed);
                key_offset = rand_key.Next() % keyrange_size_;
            }
            last_prefix_ = keyrange_size_ * keyrange_id;
            return keyrange_size_ * keyrange_id + key_offset;
        }

        int64_t GetRandomKey() {
            uint64_t rand_int = ini_rander.Next();
            int64_t key_rand;
            if (read_random_exp_range_ == 0) {
                key_rand = rand_int % FLAGS_num_;
            } else {
                const uint64_t kBigInt = static_cast<uint64_t>(1U) << 62;
                long double order = -static_cast<long double>(rand_int % kBigInt) /
                                    static_cast<long double>(kBigInt) *
                                    read_random_exp_range_;
                long double exp_ran = std::exp(order);
                uint64_t rand_num =
                        static_cast<int64_t>(exp_ran * static_cast<long double>(FLAGS_num_));
                // Map to a different number to avoid locality.
                const uint64_t kBigPrime = 0x5bd1e995;
                // Overflow is like %(2^64). Will have little impact of results.
                key_rand = static_cast<int64_t>((rand_num * kBigPrime) % FLAGS_num_);
            }
            return key_rand;
        }

        uint64_t Next();

        uint64_t Last() { return last_; }

        uint64_t LastPrefix() override { return last_prefix_; }
    };

    inline uint64_t TwoTermExpKeysGenerator::Next() {
        auto ini_rand = GetRandomKey();
        last_ = DistGetKeyID(ini_rand, key_dist_a_, key_dist_b_);
        return last_;
    }

    class SkewedLatestGenerator : public Generator<uint64_t> {
    public:
        SkewedLatestGenerator(CounterGenerator &counter) :
                basis_(counter), zipfian_(basis_.Last()) {
            Next();
        }

        uint64_t Next();

        uint64_t Last() { return last_; }

    private:
        CounterGenerator &basis_;
        ZipfianGenerator zipfian_;
        std::atomic<uint64_t> last_;
    };

    inline uint64_t SkewedLatestGenerator::Next() {
        uint64_t max = basis_.Last();
        return last_ = max - zipfian_.Next(max);
    }

} // ycsbc

#endif // YCSB_C_SKEWED_LATEST_GENERATOR_H_
