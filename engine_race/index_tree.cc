//
// Created by Harry Chen on 2019/4/14.
//

#include <cstdlib>
#include <cassert>
#include <new>

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include "index_tree.h"
#include "utils.hpp"


IndexTree::IndexTree(const std::string &filename) {
    struct stat st = {};
    index_file_fd = open(filename.c_str(), O_RDWR|O_CREAT, 0644);
    assert(index_file_fd > 0);
    fstat(index_file_fd, &st);
    index_file_size = (size_t) st.st_size;

    bool need_init = false;
    if (index_file_size == 0) {
        // no index yet
        int ret = ftruncate(index_file_fd, INIT_INDEX_FILE_SIZE);
        need_init = true;
        assert(ret == 0);
        index_file_size = INIT_INDEX_FILE_SIZE;
    }

    // round to page size
    auto map_size = (size_t) round_up(index_file_size, 4096);
    current_capacity = (uint32_t) (map_size - 2 * sizeof(uint32_t)) / sizeof(Node);

    // load index from file
    file_map = mmap(nullptr, map_size, PROT_READ | PROT_WRITE, MAP_SHARED, index_file_fd, 0);
    assert(file_map != MAP_FAILED);
    initFileMap();

    // init an empty tree
    if (need_init) {
        *root_node = -1;
        *node_count = 0;
    }

}

IndexTree::~IndexTree() {
    munmap(file_map, index_file_size);
    close(index_file_fd);
}


const IndexTree::NodeData &IndexTree::search(const PolarString &key) {
    auto current = *root_node;
    while (current != -1) {
        auto result = strcmp(key.data(), nodes[current].key);
        if (result == 0) break;
        current = result < 0 ? nodes[current].left : nodes[current].right;
    }
    if (current == -1) return INDEX_NOT_FOUND;
    else return nodes[current].data;
}


void IndexTree::insert(const PolarString &key, IndexData data) {
    // fill in a new node
    auto new_root = allocateNode();
    auto node = new (&nodes[new_root]) Node();
    node->data = data;
    memcpy(node->key, key.data(), key.size());
    node->key[key.size()] = '\0';
    // insert it to the tree
    int change;
    _insert(*root_node, new_root, change);
}


int IndexTree::balance(int32_t &root) {
    int height_change = 0;
    auto &_root = nodes[root];
    if (_root.balance_factor < -1) {
        // left unbalanced, need right rotation
        if (nodes[_root.left].balance_factor == 1) {
            // LR rotation
            height_change = rotateTwice(root, -1);
        } else {
            // R rotation
            height_change = rotateOnce(root, 1);
        }
    } else if (_root.balance_factor > 1) {
        // right unbalanced, need left rotation
        if (nodes[_root.right].balance_factor == -1) {
            // RL rotation
            height_change = rotateTwice(root, 1);
        } else {
            // L rotation
            height_change = rotateOnce(root, -1);
        }
    }

    return height_change;
}


int IndexTree::rotateOnce(int32_t &root, int direction, bool update) {

    auto old_root = root;
    auto &_old_root = nodes[root];
    auto &other_dir_id = direction == -1 ? nodes[root].right : nodes[root].left;
    auto &other_dir_node = nodes[other_dir_id];

    int height_change = other_dir_node.balance_factor == 0 ? 0 : 1;

    // do the rotation
    root = other_dir_id;
    auto &current_root = nodes[root];
    auto &current_root_this_dir = direction == -1 ? current_root.left : current_root.right;
    other_dir_id = current_root_this_dir;
    current_root_this_dir = old_root;

    if (update) {
        _old_root.balance_factor = -(direction == -1 ? --current_root.balance_factor : ++current_root.balance_factor);
    }

    return height_change;
}


int IndexTree::rotateTwice(int32_t &root, int direction) {
    if (direction == -1) {
        rotateOnce(nodes[root].left, -1);
        rotateOnce(root, 1, false);
    } else if (direction == 1){
        rotateOnce(nodes[root].right, 1);
        rotateOnce(root, -1, false);
    }

    auto &_root = nodes[root];
    nodes[_root.left].balance_factor = (int16_t) -max(_root.balance_factor, 0);
    nodes[_root.right].balance_factor = (int16_t) -min(_root.balance_factor, 0);

    return 1;
}


bool IndexTree::_insert(int32_t &root, int32_t new_node, int &height_change) {

    if (root == -1) {
        root = new_node;
        height_change = 1;
        return false;
    }

    auto &_root = nodes[root];
    auto &_new = nodes[new_node];

    int height_increase = 0;

    auto result = _new.compare(_root);

    if (result != 0) {
        auto &sub_tree_id = result == -1 ? _root.left : _root.right;
        if (_insert(sub_tree_id, new_node, height_change)) {
            return true;
        }
        height_increase = result * height_change;
    } else {
        // found existing node, replace it
        root = new_node;
        _new.left = _root.left;
        _new.right = _root.right;
        _new.balance_factor = _root.balance_factor;
        return true;
    }

    _root.balance_factor += height_increase;

    if (height_increase != 0 && _root.balance_factor != 0) {
        height_change = 1 - balance(root);
    } else {
        height_change = 0;
    }

    return false;

}


// allocate a new tree node from mapped memory
uint32_t IndexTree::allocateNode() {
    if (*node_count >= current_capacity) {
        printf("Re-mapping index file to extend size\n");
        // extend the index file size
        int ret = ftruncate(index_file_fd, index_file_size * 2);
        assert(ret == 0);
        file_map = mremap(file_map, index_file_size, index_file_size * 2, MREMAP_MAYMOVE);
        assert(file_map != MAP_FAILED);
        initFileMap();
        index_file_size *= 2;
        current_capacity = (uint32_t) (index_file_size - 2 * sizeof(uint32_t)) / sizeof(Node);
    }
    *node_count += 1;
    return *node_count - 1;
}


void IndexTree::initFileMap() {
    node_count = reinterpret_cast<uint32_t*>(file_map);
    root_node = reinterpret_cast<int32_t*>(node_count + 1);
    nodes = reinterpret_cast<Node*>(root_node + 1);
    printf("Index file map: %p %p %p\n", node_count, root_node, nodes);
}