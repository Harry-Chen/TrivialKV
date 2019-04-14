// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <string>
#include "include/engine.h"
#include "database.h"

namespace polar_race {

class EngineRace : public Engine  {
 public:
  static RetCode Open(const std::string &dir, Engine **ptr);

  explicit EngineRace(const std::string &dir);

  ~EngineRace() override;

  RetCode Write(const PolarString &key,
      const PolarString &value) override;

  RetCode Read(const PolarString &key,
      std::string *value) override;

  /*
   * NOTICE: Implement 'Range' in quarter-final,
   *         you can skip it in preliminary.
   */
  RetCode Range(const PolarString &lower,
      const PolarString &upper,
      Visitor &visitor) override;

 private:
    const static int DATABASE_SHARDS = 64;
    Database *databases[DATABASE_SHARDS] = {nullptr};
};

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
