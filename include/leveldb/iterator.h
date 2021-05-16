// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_ITERATOR_H_

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/options.h"

namespace leveldb {

class LEVELDB_EXPORT Iterator {
 public:
  Iterator();

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  virtual ~Iterator();

  // An iterator is either positioned at a key/value pair, or
  // not valid.  This method returns true iff the iterator is valid.
  virtual bool Valid() const = 0;

  // Position at the first key in the source.  The iterator is Valid()
  // after this call iff the source is not empty.
  virtual void SeekToFirst() = 0;

  // Position at the last key in the source.  The iterator is
  // Valid() after this call iff the source is not empty.
  virtual void SeekToLast() = 0;

  // Position at the first key in the source that is at or past target.
  // The iterator is Valid() after this call iff the source contains
  // an entry that comes at or past target.
  virtual void Seek(const Slice& target) = 0;

  // Moves to the next entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the last entry in the source.
  // REQUIRES: Valid()
  virtual void Next() = 0;

  // Moves to the previous entry in the source.  After this call, Valid() is
  // true iff the iterator was not positioned at the first entry in source.
  // REQUIRES: Valid()
  virtual void Prev() = 0;

  // Return the key for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice key() const = 0;

  // Return the value for the current entry.  The underlying storage for
  // the returned slice is valid only until the next modification of
  // the iterator.
  // REQUIRES: Valid()
  virtual Slice value() const = 0;

  // If an error has occurred, return it.  Else return an ok status.
  virtual Status status() const = 0;

  // Clients are allowed to register function/arg1/arg2 triples that
  // will be invoked when this iterator is destroyed.
  //
  // Note that unlike all of the preceding methods, this method is
  // not abstract and therefore clients should not override it.
  using CleanupFunction = void (*)(void* arg1, void* arg2);
  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

 private:
  // Cleanup functions are stored in a single-linked list.
  // The list's head node is inlined in the iterator.
  struct CleanupNode {
    // True if the node is not used. Only head nodes might be unused.
    bool IsEmpty() const { return function == nullptr; }
    // Invokes the cleanup function.
    void Run() {
      assert(function != nullptr);
      (*function)(arg1, arg2);
    }

    // The head node is used if the function pointer is not null.
    CleanupFunction function;
    void* arg1;
    void* arg2;
    CleanupNode* next;
  };
  CleanupNode cleanup_head_;
};

// Return an empty iterator (yields nothing).
LEVELDB_EXPORT Iterator* NewEmptyIterator();

// Return an empty iterator with the specified status.
LEVELDB_EXPORT Iterator* NewErrorIterator(const Status& status);


class ColumnFamilyIterator: public Iterator{
private:
  /* data */
public:
  ColumnFamilyIterator(ColumnFamilyHandle &cf, Iterator *iter):valid_(false),iter_(iter), cf_(cf){};
  ~ColumnFamilyIterator(){
    delete iter_;
  }

  bool Valid() const override { return valid_; }

  void Seek(const Slice& k) override { 
    iter_->Seek(cf_.GetPrefix()+k.ToString()); 
    SetValid();
  }

  void SeekToFirst() override { 
    iter_->Seek(cf_.GetPrefix()); 
    SetValid();
  }

  void SeekToLast() override { 
    iter_->SeekToLast();
    SetValid(); 
  }

  void Next() override { 
    iter_->Next();
    SetValid();
  }

  void Prev() override { 
    iter_->Prev(); 
    SetValid();
  }

  Slice key() const override { 
    return Slice(iter_->key().data()+cf_.GetPrefixSize(), iter_->key().size()-cf_.GetPrefixSize());
  }

  Slice value() const override {
    return iter_->value();
  }

  Status status() const override { return Status::OK(); }

private:

  void SetValid(){
    valid_ = iter_->Valid();
    if(valid_&&memcmp(cf_.cf_name_.c_str(), iter_->key().data(), cf_.GetPrefixSize())!=0){
      valid_ = false;
    }
  }
  bool valid_;
  Iterator* iter_;
  ColumnFamilyHandle cf_;
};

class IndexIterator: public Iterator{
public:
  explicit IndexIterator(Iterator* iter):valid_(false),cf_("Index"),iter_(iter){
  }
  ~IndexIterator(){
    delete iter_;
  }

  bool Valid() const override { return valid_; }

  void Seek(const Slice& k) override { 
    char key[9] = "00000000";
    for(int i=0;i<k.size();i++){
      key[7-i] = k[k.size()-1-i];
    }
    iter_->Seek(cf_.GetPrefix()+key); 
    SetValid();
  }

  void SeekToFirst() override { 
    iter_->Seek(cf_.GetPrefix()); 
    SetValid();
  }

  void SeekToLast() override { 
    iter_->SeekToLast();
    SetValid(); 
  }

  void Next() override { 
    iter_->Next();
    SetValid();
  }

  void Prev() override { 
    iter_->Prev(); 
    SetValid();
  }

  Slice key() const override { 
    assert(valid_);
    const char* pos = iter_->key().data()+cf_.GetPrefixSize();
    int start = GetIntPos(pos, 8);
    return Slice(pos+start, 8-start);
  }

  Slice value() const override {
    assert(valid_);
    const char* pos = iter_->key().data()+cf_.GetPrefixSize()+9;
    int start = GetIntPos(pos, 8);
    return Slice(pos+start, 8-start);
  }

  Status status() const override { return Status::OK(); }

  // 从定长的数字字符串编码中找到第一位的起始位置
  static int GetIntPos(const char* start, int size){
    int i = 0; 
    while(i < size&&start[i++]=='0') {}
    return i-1;
  }

private:
  void SetValid(){
    valid_ = iter_->Valid();
    if(valid_&&memcmp(cf_.cf_name_.c_str(), iter_->key().data(), cf_.GetPrefixSize())!=0){
      valid_ = false;
    }
  }
  bool valid_;
  ColumnFamilyHandle cf_;
  Iterator* iter_;
};


}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
