//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx)
    ,plan_(plan)
    ,child_executor_(std::move(child_executor)){
        table_infos_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
        table_indexs_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_infos_->name_);
    }

void DeleteExecutor::Init() {
    child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    if(!child_executor_->Next(tuple,rid)){
        return false;
    }
    //删除
    table_infos_->table_->MarkDelete(*rid,exec_ctx_->GetTransaction());
    // 删索引
    for (auto &table_index :table_indexs_){
        auto key_tuple = tuple->KeyFromTuple(table_infos_->schema_, table_index->key_schema_, table_index->index_->GetKeyAttrs());
        table_index->index_->DeleteEntry(key_tuple,*rid,exec_ctx_->GetTransaction());
    }
    return true; 
}

}  // namespace bustub
