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
    child_executor_(std::move(child_executor)) {
      table_info_ = exec_ctx->GetCatalog()->GetTable(plan->TableOid());
      table_indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    }

void UpdateExecutor::Init() {
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple old_tuple;
  if(!child_executor_->Next(&old_tuple,rid)){
    return false;
  }
  //回表查询
  if(!table_info_->table_->GetTuple(*rid, &old_tuple, exec_ctx_->GetTransaction())){
    return false;
  }
  //生成新
  *tuple = GenerateUpdatedTuple(old_tuple);
  
  bool isupdated = true;
  //插入
  isupdated =  table_info_->table_->UpdateTuple(*tuple,*rid,exec_ctx_->GetTransaction());
  //更改索引
  if(isupdated){
    for(auto &table_index: table_indexs_){
      auto key_old_tuple = old_tuple.KeyFromTuple(table_info_->schema_,table_index->key_schema_, table_index->index_->GetKeyAttrs());
      table_index->index_->DeleteEntry(key_old_tuple, *rid, exec_ctx_->GetTransaction());
      auto key_tuple = tuple->KeyFromTuple(table_info_->schema_,table_index->key_schema_, table_index->index_->GetKeyAttrs());
      table_index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
    }
  }
  //
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
