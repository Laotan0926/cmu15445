//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

struct HashKey {
  Value distin_bys_;
  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  bool operator==(const HashKey &other) const {
    if(distin_bys_.CompareEquals(other.distin_bys_) != CmpBool::CmpTrue) {
        return false;
    }
    return true;
  }
};

struct HashKeyValue {
  /** The aggregate values */
  std::vector<Value> sistinct_;
};

}

namespace std {
/** Implements std::hash on DistinctKey */
template <>
struct hash<bustub::HashKey> {
  std::size_t operator()(const bustub::HashKey &agg_key) const {
    size_t curr_hash = 0;
    auto &key = agg_key.distin_bys_;
    if (!key.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;

  std::unordered_map<HashKey,std::vector<std::vector<Value>>> hash_map_;
  std::vector<std::vector<Value>> outtable_buffer_;
  std::size_t next_pos_{0};
};

}  // namespace bustub
