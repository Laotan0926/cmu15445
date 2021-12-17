//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

private:
  DistinctKey MakeDistinctKey(const Tuple *tuple, const Schema *schema) {
    std::vector<Value> vals;
    std::size_t columns_size = schema->GetColumnCount();
    vals.reserve(columns_size);

    for (uint32_t i=0; i<columns_size; i++) {
      vals.emplace_back(tuple->GetValue(schema,i));
    }
    return {vals};
  }

  DistinctValue MakeDistinctValue(const Tuple *tuple, const Schema *schema) {
    std::vector<Value> vals;
    std::size_t columns_size = schema->GetColumnCount();
    vals.reserve(columns_size);

    for (uint32_t i=0; i<columns_size; i++) {
      vals.emplace_back(tuple->GetValue(schema,i));
    }
    return {vals};
  }

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** hash table*/
  std::unordered_map<DistinctKey,DistinctValue> hash_map_;
};
}  // namespace bustub
