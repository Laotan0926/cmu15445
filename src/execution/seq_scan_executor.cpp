//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
        : AbstractExecutor(exec_ctx), 
        plan_(plan), 
        table_iterator_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() {
    table_iterator_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) { 
    table_oid_t table_oid = plan_->GetTableOid();
    TableInfo* table_info =  exec_ctx_->GetCatalog()->GetTable(table_oid);
    Schema* table_schema = &table_info->schema_;
    const Schema *output_schema = plan_->OutputSchema();

    uint32_t output_schema_col_count = output_schema->GetColumnCount();
    const std::vector<Column> & output_cols = output_schema->GetColumns();
    // predicate

    const AbstractExpression *predicate = plan_->GetPredicate();

    TableHeap *table_heap = table_info->table_.get();
    Tuple original_tuple;

    while (table_iterator_ != table_heap->End()) {
        original_tuple = *table_iterator_++;
        if ((predicate != nullptr) ? predicate->Evaluate(&original_tuple, table_schema).GetAs<bool>() : true) {
        // original tuple qualified, build output tuple
        std::vector<Value> output_values;
        output_values.reserve(output_schema_col_count);
        for (size_t i = 0; i < output_schema_col_count; i++) {
            output_values.emplace_back(output_cols[i].GetExpr()->Evaluate(&original_tuple, table_schema));
        }
        assert(output_values.size() == output_schema_col_count);
        *tuple = Tuple(output_values, output_schema);
        *rid = original_tuple.GetRid();
        return true;
        }
    }
    return false; 
}
}  // namespace bustub
