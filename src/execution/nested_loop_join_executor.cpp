//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx)
    ,plan_(plan)
    ,left_executor_(std::move(left_executor))
    ,right_executor_(std::move(right_executor)){}

void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
    RID left_rid, right_rid;
    //初始left_rid
    if(left_tuple_.GetLength()==0&&!left_executor_->Next(&left_tuple_,&left_rid)){
        return false;
    }

    do{
        if(!right_executor_->Next(&right_tuple_,&right_rid)){
            if(!left_executor_->Next(&left_tuple_,&left_rid)){
                return false;
            }
            right_executor_->Init();
            if(!right_executor_->Next(&right_tuple_,&right_rid)){
                return false;
            }       
        }
    }while(plan_->Predicate()!=nullptr&&!plan_->Predicate()->EvaluateJoin(
                    &left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,right_executor_->GetOutputSchema()).GetAs<bool>());

    std::vector<Value> values;
    size_t out_schema_size = plan_->OutputSchema()->GetColumnCount();
    values.reserve(out_schema_size);
    for (size_t i =0; i< out_schema_size;i++){
        const AbstractExpression* col_expr = plan_->OutputSchema()->GetColumn(i).GetExpr();
        values.emplace_back(col_expr->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                                      right_executor_->GetOutputSchema()));
    }
    *tuple = Tuple(values, plan_->OutputSchema());
    return true; 
}

}  // namespace bustub
