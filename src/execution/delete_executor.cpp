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
    ,child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) { 
    table_oid_t table_oid =  plan_->TableOid();
    TableInfo* table_info  =  exec_ctx_->GetCatalog()->GetTable(table_oid);
    TableHeap* table_heap = table_info->table_.get();

    Tuple to_tuple;
    if(!child_executor_->Next(&to_tuple,rid)){
        return false;
    }
    // 从表中搜寻
    Tuple next_tuple;
    if(!table_heap->GetTuple(*rid,&next_tuple,exec_ctx_->GetTransaction()) ){
        return false;
    }
    //std::cout<<next_tuple.ToString(&table_info->schema_)<<std::endl;

    //删除
    table_heap->MarkDelete(*rid,exec_ctx_->GetTransaction());
    // 删索引
    std::vector<IndexInfo*> table_index_infos = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    for (auto &table_index_info:table_index_infos ){
        auto key_tuple = next_tuple.KeyFromTuple(table_info->schema_, table_index_info->key_schema_, table_index_info->index_->GetKeyAttrs());
        table_index_info->index_->DeleteEntry(key_tuple,*rid,exec_ctx_->GetTransaction());
    }

    return true; 
}

}  // namespace bustub
