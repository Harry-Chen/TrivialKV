//
// Created by Harry Chen on 2019/4/14.
//

#ifndef TRIVIALKV_DATABASE_H
#define TRIVIALKV_DATABASE_H

#include <string>
#include "include/engine.h"
#include "index_tree.h"

using polar_race::PolarString;
using polar_race::RetCode;

struct DatabaseMetadata {
    uint32_t sliceCount;
    uint32_t currentSliceNumber;
    uint32_t currentOffset;
};

class Database {
public:
    Database(const std::string &dir, int id);
    ~Database();
    RetCode write(const PolarString &key, const PolarString &value);
    RetCode read(const PolarString &key, std::string *value);
private:
    static const int MAX_SLICE_COUNT = 1 << 12;
    static const int SLICE_SIZE = 32 * 1024 * 1024;
    pthread_rwlock_t rwlock;
    int id;
    std::string file_prefix;
    IndexTree *index;
    int slice_fd[MAX_SLICE_COUNT];
    char *slices[MAX_SLICE_COUNT];
    char *currentSlice;

    // memory mapped metadata
    int metadata_fd;
    DatabaseMetadata *metadata;

    void initIndex();
    void initSlices();
    int createNewSlice();
    void mapSlice(int fd, int slice_number);
};


#endif //TRIVIALKV_DATABASE_H
