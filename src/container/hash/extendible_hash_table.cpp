//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // directory
  auto directory_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());
  directory_page->SetPageId(directory_page_id_);
  // root bucket
  page_id_t root_bucket_page_id;
  buffer_pool_manager_->NewPage(&root_bucket_page_id, nullptr);
  // add root bucket
  directory_page->SetBucketPageId(0,root_bucket_page_id);

  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(root_bucket_page_id, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_id =  Hash(key) & dir_page->GetGlobalDepthMask();
  return bucket_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_id = KeyToDirectoryIndex(key,dir_page);
  page_id_t page_id  = dir_page->GetBucketPageId(bucket_id);
  return page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  HashTableDirectoryPage * dir_page = reinterpret_cast<HashTableDirectoryPage *>(
    buffer_pool_manager_->FetchPage(directory_page_id_,nullptr)->GetData());
  return dir_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  HASH_TABLE_BUCKET_TYPE* page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(
    buffer_pool_manager_->FetchPage(bucket_page_id,nullptr)->GetData());
  return page;

}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage * directory_page = FetchDirectoryPage();
  page_id_t page_id = KeyToPageId(key, directory_page);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(page_id);
  bool flag = bucket_page->GetValue(key,comparator_,result);

  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  buffer_pool_manager_->UnpinPage(page_id, false, nullptr);
  table_latch_.RUnlock();
  return flag;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id =  dir_page->GetBucketPageId(bucket_id);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);
  if(bucket_page->Insert(key, value, comparator_)){
    buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id,true,nullptr);
    table_latch_.WUnlock();
    return true;
  }
  bool flag = bucket_page->IsFull();
  //table_latch_.WUnlock();
  if(flag){
    //is full
    if(SplitInsert(transaction, key, value)){
      buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
      buffer_pool_manager_->UnpinPage(bucket_page_id,false,nullptr);
      table_latch_.WUnlock();
      return true;
    }
  }
  // dupli and SplitInsert false
  buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id,false,nullptr);
  table_latch_.WUnlock();
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id =  dir_page->GetBucketPageId(bucket_id);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);
  // local depth == global depth
  if(dir_page->GetLocalDepth(bucket_id)==dir_page->GetGlobalDepth()){
    if(dir_page->CanIncrGlobalDepth()){
      uint32_t  current_bucket_size = dir_page->Size();
      for(uint32_t temp_bucket_id = 0; temp_bucket_id < current_bucket_size; temp_bucket_id++){
        dir_page->SetBucketPageId(temp_bucket_id+current_bucket_size, dir_page->GetBucketPageId(temp_bucket_id));
        dir_page->SetLocalDepth(temp_bucket_id+current_bucket_size, dir_page->GetLocalDepth(temp_bucket_id) );
      }
      dir_page->IncrGlobalDepth();
    }else{
      buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
      buffer_pool_manager_->UnpinPage(bucket_page_id,false,nullptr);
      return false;
    }    
  }

  // local depth < global depth
  if(dir_page->GetLocalDepth(bucket_id)<dir_page->GetGlobalDepth()){
    // local depth < global depth
    page_id_t new_buctet_page_id;
    HASH_TABLE_BUCKET_TYPE* new_buctet_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE*>(
      buffer_pool_manager_->NewPage(&new_buctet_page_id, nullptr)->GetData() );
    uint32_t locale_hight_bit = 0x1<< dir_page->GetLocalDepth(bucket_id);
    uint32_t shared_bit =  bucket_id & (locale_hight_bit - 1);
    uint32_t current_bucket_size = dir_page->Size();
    for(uint32_t temp_bucket_id = shared_bit; temp_bucket_id < current_bucket_size; temp_bucket_id += locale_hight_bit){
      if(temp_bucket_id & locale_hight_bit){
        dir_page->SetBucketPageId(temp_bucket_id,new_buctet_page_id);
      }
      dir_page->IncrLocalDepth(temp_bucket_id);
    }
    //flash 
    memcpy(reinterpret_cast<void*>(new_buctet_page), reinterpret_cast<void*>(bucket_page),PAGE_SIZE);
    uint32_t bucket_occupid_size =  bucket_page->GetOccupiedSize();
    for(uint32_t bucket_idx =0; bucket_idx < bucket_occupid_size; bucket_idx++){
      if(bucket_page->IsReadable(bucket_idx)){
          //allocat
        if(KeyToPageId(bucket_page->KeyAt(bucket_idx),dir_page) == bucket_page_id){
          new_buctet_page->RemoveAt(bucket_idx);
        }else{
          bucket_page->RemoveAt(bucket_idx);
        }
      }
    }
    buffer_pool_manager_->UnpinPage(new_buctet_page_id,true,nullptr);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_,true,nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id,true, nullptr);
  table_latch_.WUnlock();
  if(Insert(transaction, key, value)){
    return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id =  dir_page->GetBucketPageId(bucket_id);
  HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);

  bool flag = bucket_page->Remove(key, value, comparator_);
  if(flag&&bucket_page->IsEmpty()){
    // removed
    Merge(transaction, key, value);

    buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
    buffer_pool_manager_->UnpinPage(bucket_page_id,true,nullptr);
    table_latch_.WUnlock();
    return flag;
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_,false,nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id,true,nullptr);
  table_latch_.WUnlock();
  return flag;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage * dir_page = FetchDirectoryPage();
  uint32_t bucket_id = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id =  dir_page->GetBucketPageId(bucket_id);
  //HASH_TABLE_BUCKET_TYPE * bucket_page = FetchBucketPage(bucket_page_id);
  uint32_t bucket_local_depth =  dir_page->GetLocalDepth(bucket_id);
  // root bucket
  if(bucket_local_depth==0){
    return;
  }
  // not root bucket, find the other bucket
  uint32_t other_bucket_id =  bucket_id^(0x1<<(bucket_local_depth-1));
  page_id_t other_bucket_page_id = dir_page->GetBucketPageId(other_bucket_id);
  if(dir_page->GetLocalDepth(other_bucket_id)==bucket_local_depth){
    uint32_t shared = bucket_id &((0x1<<(bucket_local_depth-1))-1);
    uint32_t current_bucket_size = dir_page->Size();
    for(uint32_t temp_bucket_id = shared; temp_bucket_id < current_bucket_size; temp_bucket_id+=(0x1<<(bucket_local_depth-1)) ){
      dir_page->SetBucketPageId(temp_bucket_id, other_bucket_page_id);
      dir_page->DecrLocalDepth(temp_bucket_id);
    }
    buffer_pool_manager_->DeletePage(bucket_page_id);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_,true,nullptr);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  table_latch_.RUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
