//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx)
    , plan_(plan)
    , child_(std::move(child))
    , aht_(plan_->GetAggregates(),plan_->GetAggregateTypes())
    , aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
    child_->Init();
    Tuple tuple;
    RID rid;
    bool is_group_by = !plan_->GetGroupBys().empty();
    auto group_bys = plan_->GetGroupBys();
    auto aggr_exprs = plan_->GetAggregates();
    std::vector<Value> keys;
    keys.reserve(group_bys.size());
    std::vector<Value> vals;
    vals.reserve(aggr_exprs.size());

    while(child_->Next(&tuple, &rid)){
        keys.clear();
        if(is_group_by){
            for(auto &group_by: group_bys){
                keys.emplace_back(group_by->Evaluate(&tuple,child_->GetOutputSchema()));
            }
        }
        vals.clear();
        for(auto &aggr_expr: aggr_exprs){
            vals.emplace_back(aggr_expr->Evaluate(&tuple,child_->GetOutputSchema()));
        }
        aht_.InsertCombine(AggregateKey{keys},AggregateValue{vals});
    }
    aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) { 
    std::vector<Value> group_bys;
    std::vector<Value> aggregates;
    do{
        if(aht_iterator_== aht_.End()){
            return false;
        }
        group_bys = aht_iterator_.Key().group_bys_;
        aggregates = aht_iterator_.Val().aggregates_;
        ++aht_iterator_;
    }while(plan_->GetHaving()!=nullptr &&
                    !plan_->GetHaving()->EvaluateAggregate(group_bys,aggregates).GetAs<bool>());
    // 生成输出元组
    std::vector<Value> values;
    size_t column_count = plan_->OutputSchema()->GetColumnCount();
    for(size_t i = 0 ;i<column_count ;i++){
        const AbstractExpression* col_expr = plan_->OutputSchema()->GetColumn(i).GetExpr();
        values.emplace_back(col_expr->EvaluateAggregate(group_bys, aggregates));
    }
    *tuple = Tuple{values, plan_->OutputSchema()};
    return true;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
