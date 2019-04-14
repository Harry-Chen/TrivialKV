//
// Created by Harry Chen on 2019/4/14.
//

#include <pthread.h>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "database.h"

Database::Database(const std::string &dir, int id) {
    pthread_rwlock_init(&rwlock, nullptr);
    file_prefix = dir + "." + std::to_string(id);
    initIndex();
    initSlices();
}

RetCode Database::write(const PolarString &key, const PolarString &value) {
    pthread_rwlock_wrlock(&rwlock);
    auto data_length = (uint16_t) value.size();
    if (data_length + *currentOffset > SLICE_SIZE) {
        // use a new slice
        auto new_slice_fd = createNewSlice();
        mapSlice(new_slice_fd, *currentSliceNumber);
    }
    memcpy(slices[*currentSliceNumber] + *currentOffset, value.data(), data_length);
    index->insert(key, {(int32_t) *currentSliceNumber, *currentOffset, data_length});
    *currentOffset += value.size();
    pthread_rwlock_unlock(&rwlock);
    return polar_race::kSucc;
}

RetCode Database::read(const PolarString &key, std::string *value) {
    pthread_rwlock_rdlock(&rwlock);
    auto result = index->search(key);
    if (result.slice == -1) {
        pthread_rwlock_unlock(&rwlock);
        return polar_race::kNotFound;
    }
    auto slice = slices[result.slice];
    value->assign(slice + result.offset, result.length);
    pthread_rwlock_unlock(&rwlock);
    return polar_race::kSucc;
}

void Database::initIndex() {
    auto index_filename = file_prefix + ".index";
    index = new IndexTree(index_filename);
}

void Database::initSlices() {
    int metadata_fd = open((file_prefix + ".metadata").c_str(), O_RDWR|O_CREAT);
    assert(metadata_fd > 0);
    struct stat st = {};
    fstat(metadata_fd, &st);

    bool need_init = false;

    if (st.st_size == 0) {
        ftruncate(metadata_fd, 4096);
        need_init = true;
    }

    metadata = mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE, metadata_fd, 0);
    assert(metadata != MAP_FAILED);
    sliceCount = reinterpret_cast<uint32_t*>(metadata);
    currentSliceNumber = sliceCount + 1;
    currentOffset = currentSliceNumber + 1;

    if (need_init) {
        *sliceCount = 0;
        auto fd = createNewSlice();
        mapSlice(fd, 0);
    } else {
        for (int i = 0; i < *sliceCount; ++i) {
            auto filename = file_prefix + "." + std::to_string(i) + ".data";
            int data_fd = open(filename.c_str(), O_RDONLY);
            assert(data_fd > 0);
            mapSlice(data_fd, i);
        }
    }


}

int Database::createNewSlice() {
    auto new_slice_id = *sliceCount;
    auto filename = file_prefix + "." + std::to_string(new_slice_id) + ".data";
    int data_fd = open(filename.c_str(), O_RDWR|O_CREAT);
    assert(data_fd > 0);
    ftruncate(data_fd, SLICE_SIZE);
    *currentSliceNumber = new_slice_id;
    *currentOffset = 0;
    (*sliceCount)++;
    return data_fd;
}

void Database::mapSlice(int fd, int slice_number) {
    auto data_mapped = mmap(nullptr, SLICE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0);
    assert(data_mapped != MAP_FAILED);
    slices[slice_number] = (char*) data_mapped;
}

