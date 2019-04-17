//
// Created by Harry Chen on 2019/4/14.
//

#ifndef TRIVIALKV_UTILS_H
#define TRIVIALKV_UTILS_H

#include <cassert>

using polar_race::PolarString;

#define inline inline __attribute__((always_inline))

// at most 64 concurrent access
const int DATABASE_SHARDS = 1 << 7;

inline int get_shard_number(const PolarString &key) {
    auto shard_bits = __builtin_ctz(DATABASE_SHARDS);
//    assert(shard_bits <= 8);
    auto shard_num = 0;
    shard_num = (uint8_t) (key.data()[0]) >> (8 - shard_bits);
    return shard_num;
}


inline long round_up(long a, long b) {
    return ((a + b - 1) / b) * b;
}

inline int min(int a, int b) {
    return  (a < b) ? a : b;
}

// Return the maximum of two numbers
inline int max(int a, int b) {
    return  (a > b) ? a : b;
}

inline int fast_string_cmp(const char* a, const char *b) {
    auto fast_this = reinterpret_cast<const int64_t*>(a);
    auto fast_that = reinterpret_cast<const int64_t*>(b);
    auto fast_result = *fast_this - *fast_that;
    if (fast_result != 0) return fast_result < 0 ? -1 : 1;
    auto result = strcmp(a, b);
    return result < 0 ? -1 : result > 0 ? 1 : 0;
}

#undef inline

#endif //TRIVIALKV_UTILS_H
