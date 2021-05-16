// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

class Arena;

template <typename Key, class Comparator>
class SkipList {
 private:
  struct Node;

 public:
  // Create a new SkipList object that will use "cmp" for comparing keys,
  // and will allocate memory using "*arena".  Objects allocated in the arena
  // must remain allocated for the lifetime of the skiplist object.
  explicit SkipList(Comparator cmp, Arena* arena);

  explicit SkipList(Comparator cmp, Arena* arena, bool with_hashmap);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
  void Insert(const Key& key);

  // Returns true iff an entry that compares equal to key is in the list.
  bool Contains(const Key& key) const;

  void PrintTable();

  std::string MyDecoder(const char* p);

  // Iteration over the contents of a skip list
  class Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(const SkipList* list);

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const;

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const Key& key() const;

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next();

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev();

    // Advance to the first entry with a key >= target
    void Seek(const Key& target);

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst();

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast();

   private:
    const SkipList* list_;
    Node* node_;
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };

  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  Node* NewNode(const Key& key, int height);
  int RandomHeight();
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  // Return the earliest node that comes at or after key.
  // Return nullptr if there is no such node.
  //
  // If prev is non-null, fills prev[level] with pointer to previous
  // node at "level" for every level in [0..max_height_-1].
  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  // Return the latest node with a key < key.
  // Return head_ if there is no such node.
  Node* FindLessThan(const Key& key) const;

  // Return the last node in the list.
  // Return head_ if list is empty.
  Node* FindLast() const;

  // Immutable after construction
  Comparator const compare_;
  Arena* const arena_;  // Arena used for allocations of nodes

  Node* const head_;  // skiplist的前置哨兵节点

  // Modified only by Insert().  Read racily by readers, but stale
  // values are ok.
  std::atomic<int>
      max_height_;  // Height of the entire list//记录当前skiplist使用的最高高度

  // Read/written only by Insert().
  Random rnd_;
  bool with_hashmap_;
  std::unordered_map<std::string, Node*>hashmap_;
};

// Implementation details follow
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;
  int height;

  // Accessors/mutators for links.  Wrapped in methods so we can
  // add the appropriate barriers as necessary.
  Node* Next(int n) {  //获取当前节点在指定level的下一个节点
    assert(n >= 0);
    // Use an 'acquire load' so that we observe a fully initialized
    // version of the returned Node.
    return next_[n].load(std::memory_order_acquire);
  }
  void SetNext(int n, Node* x) {  // 将当前节点在指定level的下一个节点设置为x
    assert(n >= 0);
    // Use a 'release store' so that anybody who reads through this
    // pointer observes a fully initialized version of the inserted node.
    next_[n].store(x,
                   std::memory_order_release);  // 等待其它写入操作完成后才执行
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);  // 不对操作顺序做保证
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  std::atomic<Node*> next_[1];
};

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1));
  Node* ans = new (node_memory) Node(key);
  ans->height = height;
  return  ans;
}

template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  if(list_->with_hashmap_){
    uint32_t key_length;
    const char *key_t = GetVarint32Ptr((char*)target, (char*)target + 5, &key_length);
    std::string map_key(key_t, key_length-8);
//    std::unordered_map<std::string, Node*>::iterator* it = list_->hashmap_.find(map_key);
    Node* t=nullptr;
    auto it = list_->hashmap_.find(map_key);
    if(it !=list_->hashmap_.end()){
      t = it->second;
      while (list_->compare_(t->key, target) < 0){
        t = t->Next(0);
      }
    }
    node_ = t;
    return;
  }
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

template <typename Key, class Comparator>
int SkipList<Key, Comparator>::
    RandomHeight() {  //利用随机数实现每次有4分之一的概率增长高度。
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && ((rnd_.Next() % kBranching) == 0)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(
    const Key& key, Node* n) const {  // if(n->key<key || n==nullptr)return true
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key,
                                              Node** prev) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (KeyIsAfterNode(key, next)) {
      // Keep searching in this list
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;
      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}

template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena, bool with_hashmap)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef),
      with_hashmap_(with_hashmap),
      hashmap_() {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}


template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)),
      max_height_(1),
      rnd_(0xdeadbeef),
      with_hashmap_(false),
      hashmap_() {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr);
  }
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {
  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  Node* prev[kMaxHeight];
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == nullptr || !Equal(key, x->key));
  //使用随机数获取该节点的插入高度
  // 大于当前skiplist 最高高度的话，将多出的来的高度的prev 设置为前置节点
  int height = RandomHeight();
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }

  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));  // set next of x
    prev[i]->SetNext(i, x);                               // set pre of x
  }

  if(with_hashmap_){
    uint32_t key_length;
    const char *key_t = GetVarint32Ptr((char*)key, (char*)key + 5, &key_length);
    std::string map_key(key_t, key_length-8);
    hashmap_[map_key] = x; 
  }
//  PrintTable();
}

template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

template <typename Key, class Comparator>
std::string SkipList<Key, Comparator>::MyDecoder(const char* p){
  enum ValueType1 { keyTypeDeletion = 0x0, keyTypeValue = 0x1 };
  std::string ans = "Key: ";
  std::string key;
  uint32_t keylength;
  uint32_t valuelength;
  uint64_t tag;
  uint64_t seq;

  p = GetVarint32Ptr(p, p + 5, &keylength);
  keylength -= 8;
  key = std::string(p, keylength);
  ans += key;
  tag = DecodeFixed64(p + keylength);
  seq = tag >> 8;
  ans += ", sequence: " + std::to_string(seq);

  switch (static_cast<ValueType>(tag & 0xff)) {
    case kTypeValue: {
      p += keylength + 8;
      p = GetVarint32Ptr(p, p + 5, &valuelength);
      std::string value = std::string(p, valuelength);
      ans += ", Value: " + value;
      break;
    }
    case kTypeDeletion:
      ans += ", Delete";
  }
  return ans;
}

template <typename Key, class Comparator>
void SkipList<Key, Comparator>::PrintTable() {
  Node* x = head_;
  while (x->Next(0) != nullptr) {
    x = x->Next(0);
    std::cout << MyDecoder((char*)x->key) <<", height:"<<x->height<< std::endl;
  }
  std::cout << "------------------------PRINT------------------------\n";
}

// template<typename Key, class Comparator>
// void SkipList<Key, Comparator>::PrintTable(){
//
//    int level = GetMaxHeight() - 1;
//    for(int i = level; i >= 0; i--){
//        Node* next = head_->NoBarrier_Next(i);
//        if(next == nullptr)
//            continue;
//        while(true){
//            // entry format is:
//            //    internal-keylength = keylength + 8 varint32
//            //    key  char[klength]
//            //    tag      uint64   seqnumber + valuetype
//            //    vlength  varint32
//            //    value    char[vlength]
//
//            //printf key
//            const char* p = (char*)next->key;
//            uint32_t keylength;
//            p = GetVarint32Ptr(p, p + 5, &keylength);//get internal key size
//            keylength -= 8;
//            std::string key = std::string(p, keylength);
//            std::cout << "key: " << key << "\t";
//
//            //printf value
//            const uint64_t tag = DecodeFixed64(p + keylength);//get seq number
//            and value type switch (static_cast<ValueType1>(tag & 0xff))
//            {//static_cast相当于强制转换，把tag转换为valuetype类型
//                case 1: {
//                    uint32_t valuelength;
//                    p += keylength + 8;
//                    p = GetVarint32Ptr(p, p + 5, &valuelength);//get value
//                    size std::string value = std::string(p, valuelength);
//                    std::cout << "value: " << value << "\t->\t";
//                    break;
//                }
//                case 0:
//                    std::cout << "del" << "\t->\t";
//                    break;
//            }
//            next = next->NoBarrier_Next(i);
//            if(next == nullptr){
//                std::cout << "NULL\n";
//                break;
//            }
//        }
//    }
//    std::cout << "------------------------PRINT------------------------\n";
//}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
