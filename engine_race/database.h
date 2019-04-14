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

class Database {
public:
    Database(const std::string &dir, int id);
    RetCode write(const PolarString &key, const PolarString &value);
    RetCode read(const PolarString &key, std::string *value);
private:
    std::string file_prefix;
    IndexTree *index;
    void initIndex();
};


#endif //TRIVIALKV_DATABASE_H
