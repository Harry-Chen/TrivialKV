//
// Created by Harry Chen on 2019/4/14.
//

#ifndef TRIVIALKV_UTILS_H
#define TRIVIALKV_UTILS_H

#include "engine_race.h"

using polar_race::PolarString;

inline __attribute__((always_inline)) int get_shard_number(const PolarString &key, const int shard_number) {
    auto shard_bits = __builtin_ctz(shard_number);
    auto shard_num = 0;
    for (int i = 0; i < shard_bits; ++i) {
        shard_num <<= 1;
        shard_num += i < key.size() ? key[i] : 0;
    }
    return shard_num;
}


inline long round_up(long a, long b) {
    return ((a + b - 1) / b) * b;
}

#endif //TRIVIALKV_UTILS_H
