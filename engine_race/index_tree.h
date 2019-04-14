//
// Created by Harry Chen on 2019/4/14.
//

#ifndef TRIVIALKV_INDEX_TREE_H
#define TRIVIALKV_INDEX_TREE_H

#include <string>
#include <cstddef>

#include "include/polar_string.h"

using polar_race::PolarString;

struct IndexData {
    int32_t slice;
    int32_t offset;
};

const int MAX_KEY_LENGTH = 1024;

template<class T>
struct IndexItem {
    using DataType = T;
    char key[MAX_KEY_LENGTH + 1];
    T data;
    int16_t balance_factor;
    int32_t left = -1;
    int32_t right = -1;

    inline int compare(const IndexItem &rhs) const {
        return strcmp(key, rhs.key);
    }
};


class IndexTree {
public:
    using Node = IndexItem<IndexData>;
    using NodeData = Node::DataType;

    IndexTree(const std::string &filename);
    NodeData search(const PolarString &key);
    void insert(const PolarString &key, IndexData data);
private:
    void initFileMap();
    uint32_t allocateNode();
    int balance(int32_t &root);
    bool _insert(int32_t &root, int32_t new_node, int &height_change);
    int rotateOnce(int32_t &root, int direction);
    int rotateTwice(int32_t &root, int direction);

    int index_file_fd;
    size_t index_file_size;
    uint32_t current_capacity;

    void *file_map;
    uint32_t *node_count;
    int32_t *root_node;
    Node *nodes;

};

const int INIT_INDEX_FILE_SIZE = 16 * 1024 * 1024;

#endif //TRIVIALKV_INDEX_TREE_H
