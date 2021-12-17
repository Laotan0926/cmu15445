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
    child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    table_oid_t table_oid =  plan_->TableOid();
    TableInfo* table_info = exec_ctx_->GetCatalog()->GetTable(table_oid);
    TableHeap* table_heap = table_info->table_.get();
    Schema* table_schema = &table_info->schema_;
    bool isinserted = true;

    if(plan_->IsRawInsert()){
        if(insert_next>=plan_->RawValues().size()){
            return false;
        }else{
            const std::vector<Value> &raw_val = plan_->RawValuesAt(insert_next); 
            tuple = new Tuple(raw_val, table_schema);
            ++insert_next;
        }
    }else{
        if(!child_executor_->Next(tuple,rid)){
            return false;
        }
    }
    isinserted = table_heap->InsertTuple(*tuple,rid,exec_ctx_->GetTransaction());
    // index insert
    if(isinserted){
        std::vector<IndexInfo*> table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        for(auto &table_index: table_indexes){
            table_index->index_->InsertEntry(tuple->KeyFromTuple(*table_schema,table_index->key_schema_, table_index->index_->GetKeyAttrs() )
                            , *rid, exec_ctx_->GetTransaction());
        }
    }
    return isinserted;
}
}  // namespace bustub
