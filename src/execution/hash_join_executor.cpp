//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , left_executor_(std::move(left_child))
    , right_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    //hash_map_.clear();
    Tuple tuple;
    RID rid;
    Value value;

    // while(left_executor_->Next(&tuple,&rid)){
    //     std::cout<<tuple.ToString(plan_->GetChildAt(0)->OutputSchema())<<std::endl;
    // }


}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
    Tuple left_tuple, right_tuple;
    RID left_rid, right_rid;
    return false;

}

}  // namespace bustub
