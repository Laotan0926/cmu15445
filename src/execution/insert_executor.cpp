//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    child_executor_(std::move(child_executor)) {
        table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
        table_indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    }

void InsertExecutor::Init() {
    if(child_executor_!=nullptr){
        child_executor_->Init();
    }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    bool isinserted = true;

    if(plan_->IsRawInsert()){
        if(insert_next_>=plan_->RawValues().size()){
            return false;
        }else{
            const std::vector<Value> &raw_val = plan_->RawValuesAt(insert_next_); 
            *tuple = Tuple(raw_val, &table_info_->schema_);
            ++insert_next_;
        }
    }else{
        if(!child_executor_->Next(tuple,rid)){
            return false;
        }
    }
    isinserted = table_info_->table_->InsertTuple(*tuple,rid,exec_ctx_->GetTransaction());
    // 索引插入
    if(isinserted){
        for(auto &table_index: table_indexs_){
            auto key_index = tuple->KeyFromTuple(table_info_->schema_,table_index->key_schema_, table_index->index_->GetKeyAttrs());
            table_index->index_->InsertEntry( key_index, *rid, exec_ctx_->GetTransaction());
        }
    }
    return isinserted;
}
}  // namespace bustub
