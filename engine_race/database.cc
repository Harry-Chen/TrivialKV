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

Database::Database(const std::string &dir, int id): id(id) {
    pthread_rwlock_init(&rwlock, nullptr);
    file_prefix = dir + "." + std::to_string(id);
//    printf("Database shard %d initing...\n", id);
    initIndex();
    initSlices();
}


Database::~Database() {
    delete index;
    // unmap all opened files
    for (int i = 0; i < metadata->sliceCount; ++i) {
        munmap(slices[i], SLICE_SIZE);
        close(slice_fd[i]);
    }
    munmap(metadata, 4096);
    close(metadata_fd);
}

RetCode Database::write(const PolarString &key, const PolarString &value) {
    pthread_rwlock_wrlock(&rwlock);
//    printf("DB Shard %d write %s with %s\n", id, key.data(), value.data());
    auto data_length = (uint16_t) value.size();
    if (__glibc_unlikely(data_length + metadata->currentOffset > SLICE_SIZE)) {
        // use a new slice
        auto new_slice_fd = createNewSlice();
        mapSlice(new_slice_fd, metadata->currentSliceNumber);
        currentSlice = slices[metadata->currentSliceNumber];
    }
    memcpy(currentSlice + metadata->currentOffset, value.data(), data_length);
    index->insert(key, {(int32_t) metadata->currentSliceNumber, metadata->currentOffset, data_length});
    metadata->currentOffset += value.size();
    pthread_rwlock_unlock(&rwlock);
    return polar_race::kSucc;
}

RetCode Database::read(const PolarString &key, std::string *value) {
    pthread_rwlock_rdlock(&rwlock);
//    printf("DB Shard %d read %s\n", id, key.data());
    auto result = index->search(key);
    if (__glibc_unlikely(result.slice == -1)) {
        pthread_rwlock_unlock(&rwlock);
//        printf("Not Found\n");
        return polar_race::kNotFound;
    }
    auto slice = slices[result.slice];
    value->assign(slice + result.offset, result.length);
//    printf("Found %s\n", value->c_str());
    pthread_rwlock_unlock(&rwlock);
    return polar_race::kSucc;
}

void Database::initIndex() {
    auto index_filename = file_prefix + ".index";
    index = new IndexTree(index_filename);
}

void Database::initSlices() {
    metadata_fd = open((file_prefix + ".metadata").c_str(), O_RDWR|O_CREAT, 0644);
    assert(metadata_fd > 0);
    struct stat st = {};
    fstat(metadata_fd, &st);

    bool need_init = false;

    if (st.st_size == 0) {
        ftruncate(metadata_fd, sizeof(DatabaseMetadata));
        need_init = true;
    }

    metadata = reinterpret_cast<DatabaseMetadata*>(mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, metadata_fd, 0));
    assert(metadata != MAP_FAILED);

    if (need_init) {
        metadata->sliceCount = 0;
        slice_fd[0] = createNewSlice();
        mapSlice(slice_fd[0], 0);
        currentSlice = slices[0];
    } else {
        for (int i = 0; i < metadata->sliceCount; ++i) {
            auto filename = file_prefix + "." + std::to_string(i) + ".data";
            slice_fd[i] = open(filename.c_str(), O_RDWR, 0644);
            assert(slice_fd[i] > 0);
            mapSlice(slice_fd[i], i);
        }
//        printf("%d %d %d\n", *sliceCount, *currentSliceNumber, *currentOffset);
        currentSlice = slices[metadata->sliceCount - 1];
    }


}

int Database::createNewSlice() {
    auto new_slice_id = metadata->sliceCount;
    auto filename = file_prefix + "." + std::to_string(new_slice_id) + ".data";
    int data_fd = open(filename.c_str(), O_RDWR|O_CREAT, 0644);
    assert(data_fd > 0);
    ftruncate(data_fd, SLICE_SIZE);
    metadata->currentSliceNumber = new_slice_id;
    metadata->currentOffset = 0;
    metadata->sliceCount++;
    return data_fd;
}

void Database::mapSlice(int fd, int slice_number) {
    auto data_mapped = mmap(nullptr, SLICE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(data_mapped != MAP_FAILED);
    madvise(data_mapped, SLICE_SIZE, MADV_SEQUENTIAL);
    slices[slice_number] = (char*) data_mapped;
}

