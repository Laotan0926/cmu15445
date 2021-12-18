//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  for(size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      break;
    }
    if(IsReadable(bucket_idx) && cmp(key,KeyAt(bucket_idx))==0 ){
      (*result).push_back(array_[bucket_idx].second);
    }
  }
  return (*result).size()>0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  std::vector<uint32_t> available_bucket_idx;
  uint32_t bucket_idx =0;
  // if exist
  for( ; bucket_idx< BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      break;
    }
    if(IsReadable(bucket_idx)){
      if( cmp(key, KeyAt(bucket_idx))==0 && value == ValueAt(bucket_idx) ){
        return false;
      }
    }else{
      available_bucket_idx.push_back(bucket_idx);
    }
  }

  uint32_t avail_idx;
  if(available_bucket_idx.size()>0){
    avail_idx = available_bucket_idx[0];
  }else{
    if(bucket_idx >= BUCKET_ARRAY_SIZE){
      // full
      return false;
    }else{
      avail_idx = bucket_idx;
      SetOccupied(avail_idx);
    }
  }
  array_[avail_idx].first = key;
  array_[avail_idx].second = value;
  SetReadable(avail_idx);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for(uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      break;
    }
    if(IsReadable(bucket_idx)){
      if( cmp(key,KeyAt(bucket_idx))==0 && value==ValueAt(bucket_idx) ){
        RemoveAt(bucket_idx);
        return true;
      }
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  readable_[bucket_idx>>0x3] &= ~(0x1 << (bucket_idx & 0x7));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  return occupied_[bucket_idx>>0x3] & 0x1 << (bucket_idx & 0x7);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::GetOccupiedSize() const{
  uint32_t size = 0;
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      break;
    }
    size++;
  }
  return size;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  occupied_[bucket_idx>>0x3] |= 0x1 << (bucket_idx & 0x7);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  return readable_[bucket_idx>>0x3] & (0x1 << (bucket_idx & 0x7));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  readable_[bucket_idx>>0x3] |= 0x1 << (bucket_idx & 0x7); 
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for(uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      return false;
    }
    if(!IsReadable(bucket_idx)){
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t size = 0;
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      break;
    }
    if(IsReadable(bucket_idx)){
      size++;
    }
  }
  return size;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  for (uint32_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++){
    if(!IsOccupied(bucket_idx)){
      break;
    }
    if(IsReadable(bucket_idx)){
      return false;
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;


}  // namespace bustub
