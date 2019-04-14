//
// Created by Harry Chen on 2019/4/14.
//

#include <pthread.h>
#include "database.h"

Database::Database(const std::string &dir, int id) {
    file_prefix = dir + "." + std::to_string(id);
    initIndex();
}

RetCode Database::write(const PolarString &key, const PolarString &value) {
    return polar_race::kSucc;
}

RetCode Database::read(const PolarString &key, std::string *value) {
    return polar_race::kNotFound;
}

void Database::initIndex() {
    auto index_filename = file_prefix + ".index";
    index = new IndexTree(index_filename);
}
