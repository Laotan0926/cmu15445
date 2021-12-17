//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
    plan_(plan),
    table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid()) ),
    child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  TableHeap* table_heap = table_info_->table_.get();
  const Schema* table_schema = &table_info_->schema_;

  Tuple old_tuple;
  if(!child_executor_->Next(&old_tuple,rid)){
    return false;
  }

  if(!table_info_->table_->GetTuple(*rid, &old_tuple, exec_ctx_->GetTransaction())){
    return false;
  }

  *tuple = GenerateUpdatedTuple(old_tuple);
  
  bool isupdated = true;
  //插入
  isupdated =  table_heap->UpdateTuple(*tuple,*rid,exec_ctx_->GetTransaction());
  //更改索引
  if(isupdated){
    std::vector<IndexInfo*> table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for(auto &table_index: table_indexes){
      table_index->index_->DeleteEntry(old_tuple.KeyFromTuple(*table_schema,table_index->key_schema_, table_index->index_->GetKeyAttrs())
              , *rid, exec_ctx_->GetTransaction());
      table_index->index_->InsertEntry(tuple->KeyFromTuple(*table_schema,table_index->key_schema_, table_index->index_->GetKeyAttrs())
              , *rid, exec_ctx_->GetTransaction());
    }
  }

  //std::cout<<tuple->ToString(table_schema)<<std::endl;
  return isupdated; 
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
