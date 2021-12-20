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
    hash_map_.clear();
    outtable_buffer_.clear();
    next_pos_ = 0;

    Tuple left_tuple;
    RID left_rid;
    std::vector<Value> vals;
    Value val;

    while(left_executor_->Next(&left_tuple,&left_rid)){
        val = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, plan_->GetLeftPlan()->OutputSchema());
        auto left_col_count = plan_->GetLeftPlan()->OutputSchema()->GetColumnCount();
        vals.clear();
        vals.reserve(left_col_count);
        for(uint32_t i = 0; i < left_col_count; i++){
            vals.emplace_back(left_tuple.GetValue(plan_->GetLeftPlan()->OutputSchema(), i));
        }
        HashKey key{val};
        if(hash_map_.count(key)>0){
            hash_map_[key].emplace_back(std::move(vals));
        }else{
            hash_map_.insert({key,{vals}});
        }
    }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
    bool is_find = false;
    if(next_pos_>=outtable_buffer_.size()){
        while(right_executor_->Next(tuple,rid)){
            Value val = plan_->RightJoinKeyExpression()->Evaluate(tuple, plan_->GetRightPlan()->OutputSchema());
            auto iter = hash_map_.find(HashKey{val});
            if(iter!=hash_map_.cend()){
                outtable_buffer_ = iter->second;
                next_pos_ = 0;
                is_find = true;
                break;
            }
        }
        if(!is_find){
            return false;
        }
    }
    std::vector<Value> values;
    values.reserve(plan_->OutputSchema()->GetColumnCount());
    for (const auto &column : plan_->OutputSchema()->GetColumns()) {
        auto column_expr = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
        if (column_expr->GetTupleIdx() == 0) {
            values.push_back(outtable_buffer_[next_pos_][column_expr->GetColIdx()]);
        } else {
            values.push_back(tuple->GetValue(plan_->GetRightPlan()->OutputSchema(), column_expr->GetColIdx()));
        }
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    next_pos_++;
    return true;
}

}  // namespace bustub
