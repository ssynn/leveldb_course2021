#include <cstdio>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

using namespace std;

class MutexLock {
    public:
    explicit MutexLock(mutex* mu) : mu_(mu) {
      this->mu_->lock();
    }
    ~MutexLock() { this->mu_->unlock(); }

    MutexLock(const MutexLock&) = delete;
    MutexLock& operator=(const MutexLock&) = delete;

    private:
    mutex* mu_;
};

class List {
 public:
  struct Node;

  List();

  bool Add(int v);

  bool Contain(int v);

  bool Delete(int v);

  string ToString();

 private:
  Node* header;
};

struct List::Node {
  int value;
  Node* next;
  mutex mu;
  Node() {
    value = 0;
    next = nullptr;
  }

  explicit Node(int v) {
    value = v;
    next = nullptr;
  }
};

List::List() { header = new Node(); }

// TODO:
bool List::Add(int v) {
  
}

// TODO:
bool List::Contain(int v) {
 
}

// TODO:
bool List::Delete(int v) {
  
}

string List::ToString() {
  string ans = "Header";
  Node* p = header->next;
  while (p != nullptr) {
    ans += " -> " + std::to_string(p->value);
    p = p->next;
  }
  return ans;
}

TEST(List, Add) {
  List* l = new List();
  ASSERT_EQ(l->Add(1), true);
  ASSERT_EQ(l->Add(2), true);
  ASSERT_EQ(l->Add(-2323), true);
  ASSERT_EQ(l->Add(2), false);
  cout << l->ToString() << endl;
  ASSERT_EQ(l->Contain(1), true);
  ASSERT_EQ(l->Contain(2), true);
  ASSERT_EQ(l->Contain(-2323), true);
  ASSERT_EQ(l->Contain(12313), false);
  ASSERT_EQ(l->Contain(34), false);
  delete l;
}

TEST(List, Delete) {
  List* l = new List();
  l->Add(1);
  l->Add(2);
  l->Add(-2323);
  l->Add(123);
  l->Add(-1231234);
  cout << l->ToString() << endl;
  l->Delete(1);
  l->Delete(2);
  l->Delete(4);
  l->Delete(-1231234);
  ASSERT_EQ(l->Contain(1), false);
  ASSERT_EQ(l->Contain(2), false);
  ASSERT_EQ(l->Contain(4), false);
  ASSERT_EQ(l->Contain(-2323), true);
  ASSERT_EQ(l->Contain(123), true);
  cout << l->ToString() << endl;
  delete l;
}

TEST(ListMultiThread, Add) {
  int thread_number = 100;
  int insert_num = 100;
  List* l_add = new List();
  vector<thread> add_thread;
  add_thread.reserve(thread_number);
  for (int i = thread_number - 1; i >= 0; i--) {
    add_thread.emplace_back(
        [](List* l, int i, int insert_num) {
          for (int k = i * insert_num; k < i * insert_num + insert_num; k++) {
            l->Add(k);
          }
        },
        l_add, i, insert_num);
  }
  for (auto& t : add_thread) {
    t.join();  // wait here
  }
  // Check
  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l_add->Contain(i), true);
  }
  delete l_add;
}

TEST(ListMultiThread, Delete) {
  int thread_number = 10;
  int insert_num = 100;
  List* l_delete = new List();

  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l_delete->Add(i), true);
  }

  vector<thread> add_thread;
  add_thread.reserve(thread_number);
  for (int i = 0; i < thread_number; i++) {
    add_thread.emplace_back(
        [](List* l, int i, int insert_num) {
          for (int k = i * insert_num; k < i * insert_num + insert_num; k++) {
            ASSERT_EQ(l->Delete(k), true);
          }
        },
        l_delete, i, insert_num);
  }
  for (auto& t : add_thread) {
    t.join();
  }
  // Check
  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l_delete->Contain(i), false);
  }
  delete l_delete;
}

TEST(ListMultiThread, InsertAndDelete){
  int thread_number = 10;
  int insert_num = 100;
  List* l_ad = new List();

  vector<thread> add_thread;
  add_thread.reserve(thread_number);
  for (int i = 0; i < thread_number; i++) {
    add_thread.emplace_back(
        [](List* l, int i, int insert_num) {
          for (int k = i * insert_num; k < i * insert_num + insert_num; k++) {
            ASSERT_EQ(l->Add(k), true);
            ASSERT_EQ(l->Delete(k), true);
          }
        },
        l_ad, i, insert_num);
  }
  for (auto& t : add_thread) {
    t.join();
  }

  vector<thread>add_thread2;
  add_thread2.reserve(thread_number);
  for (int i = 0; i < thread_number; i++) {
    add_thread2.emplace_back(
        [](List* l, int i, int insert_num) {
          for (int k = 0; k < insert_num ; k++) {
            l->Add(k);
            l->Delete(k);
          }
        },
        l_ad, i, insert_num);
  }
  for (auto& t : add_thread2) {
    t.join();
  }

  // Check
  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l_ad->Contain(i), false);
  }
}

int main() {
  testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}