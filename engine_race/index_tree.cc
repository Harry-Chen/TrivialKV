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


int IndexTree::rotateOnce(int32_t &root, int direction) {
//    printf("Rotate once\n");
    auto old_root = root;
    auto &_old_root = nodes[root];
    auto &old_root_other_dir = direction == -1 ? _old_root.right : _old_root.left;
    auto &_other_dir = nodes[old_root_other_dir];

    int height_change = _other_dir.balance_factor == 0 ? 0 : 1;

    // do the rotation
    root = old_root_other_dir;
    auto &_new_root = nodes[root];
    auto &new_root_this_dir = direction == -1 ? _new_root.left : _new_root.right;
    old_root_other_dir = new_root_this_dir;
    new_root_this_dir = old_root;

    _old_root.balance_factor = -(direction == -1 ? --_new_root.balance_factor : ++_new_root.balance_factor);

    return height_change;
}


int IndexTree::rotateTwice(int32_t &root, int direction) {
    auto old_root = root;
    auto &_old_root = nodes[root];
    auto old_right = _old_root.right;
    auto &_old_right = nodes[old_right];
    auto old_left = _old_root.left;
    auto &_old_left = nodes[old_left];

//    printf("Before rotate twice %d root: %d %d %d\n", direction, root, _old_root.left, _old_root.right);
//    if (_old_root.left != -1) {
//        printf("\tleft: %d %d %d\n", _old_root.left, _old_left.left, _old_left.right);
//    }
//    if (_old_root.right != -1) {
//        printf("\tright: %d %d %d\n", _old_root.right, _old_right.left, _old_right.right);
//    }
    if (direction == -1) {
        root = _old_left.right;
        auto &_new_root = nodes[root];
        // re-attach
        _old_root.left = _new_root.right;
        _old_left.right = _new_root.left;
        _new_root.left = old_left;
        _new_root.right = old_root;
    } else if (direction == 1){
        root = _old_right.left;
        auto &_new_root = nodes[root];
        // re-attach
        _old_root.right = _new_root.left;
        _old_right.left = _new_root.right;
        _new_root.right = old_right;
        _new_root.left = old_root;
    }

    auto &new_root = nodes[root];
    auto &_new_left = nodes[new_root.left];
    auto &_new_right = nodes[new_root.right];
    _new_left.balance_factor = (int16_t) -max(new_root.balance_factor, 0);
    _new_right.balance_factor = (int16_t) -min(new_root.balance_factor, 0);
    new_root.balance_factor = 0;
//
//    printf("After rotate twice: %d %d %d\n", root, new_root.left, new_root.right);
//    if (new_root.left != -1) {
//        printf("\tleft: %d %d %d\n", new_root.left, _new_left.left, _new_left.right);
//    }
//    if (new_root.right != -1) {
//        printf("\tright: %d %d %d\n", new_root.right, _new_right.left, _new_right.right);
//    }
    return 1;
}


bool IndexTree::_insert(int32_t &root, int32_t new_node, int &balance_change) {

    if (root == -1) {
        root = new_node;
        balance_change = 1;
        return false;
    }

    auto &_root = nodes[root];
    auto &_new = nodes[new_node];
    balance_change = 0;
    int height_increase = 0;

//    printf("Insert querying: %d left %d right %d key %s\n", root, _root.left, _root.right, _root.key);

    auto result = _new.compare(_root);

    if (result != 0) {
        auto &sub_tree_id = result == -1 ? _root.left : _root.right;
        if (_insert(sub_tree_id, new_node, balance_change)) {
            return true;
        }
        height_increase = result * balance_change;
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
        balance_change = 1 - balance(root);
    } else {
        balance_change = 0;
    }

    return false;

}


// allocate a new tree node from mapped memory
uint32_t IndexTree::allocateNode() {
    if (*node_count >= current_capacity) {
//        printf("Re-mapping index file to extend size\n");
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
//    printf("Index file map: %p %p %p\n", node_count, root_node, nodes);
}