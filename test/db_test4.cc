#include <cstdio>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

using namespace std;

class List {
 public:
  struct Node;

  List();

  bool Add(int v);

  bool Contain(int v);

  bool Delete(int v);

  string to_string();

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
  if (Contain(v)) return false;  // 值不能重复

  Node* p = header;

  while (p->next != nullptr) {
    if (p->next->value >= v) break;
    p = p->next;
  }

  Node* t = new Node(v);
  t->next = p->next;
  p->next = t;
  return true;
}

// TODO:
bool List::Contain(int v) {
  Node* p = header->next;
  while (p != nullptr) {
    if (p->value != v)
      p = p->next;
    else
      return true;
  }
  return false;
}

// TODO:
bool List::Delete(int v) {
  Node* p = header;
  while (p->next != nullptr) {
    if (p->next->value != v) {
      p = p->next;
      continue;
    }
    Node* t = p->next;
    p->next = t->next;
    delete t;
    return true;
  }
  return false;
}

string List::to_string() {
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
  cout << l->to_string() << endl;
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
  cout << l->to_string() << endl;
  l->Delete(1);
  l->Delete(2);
  l->Delete(4);
  l->Delete(-1231234);
  ASSERT_EQ(l->Contain(1), false);
  ASSERT_EQ(l->Contain(-2323), true);
  ASSERT_EQ(l->Contain(123), true);
  cout << l->to_string() << endl;
  delete l;
}

TEST(ListMultiThread, Add) {
  int thread_number = 100;
  int insert_num = 100;
  List* l = new List();
  vector<thread> add_thread;
  add_thread.reserve(thread_number);
  for (int i = thread_number - 1; i >= 0; i--) {
    add_thread.emplace_back(
        [](List* l, int i, int insert_num) {
          for (int k = i * insert_num; k < i * insert_num + insert_num; k++) {
            l->Add(k);
          }
        },
        l, i, insert_num);
  }
  for (auto& t : add_thread) {
    t.join();  // wait here
  }
  // Check
  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l->Contain(i), true);
  }
}

TEST(ListMultiThread, Delete) {
  int thread_number = 10;
  int insert_num = 100;
  List* l = new List();

  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l->Add(i), true);
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
        l, i, insert_num);
  }
  for (auto& t : add_thread) {
    t.join();
  }
  // Check
  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l->Contain(i), false);
  }
}

TEST(ListMultiThread, InsertAndDelete){
  int thread_number = 10;
  int insert_num = 100;
  List* l = new List();

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
        l, i, insert_num);
  }
  for (auto& t : add_thread) {
    t.join();
  }
  // Check
  for (int i = 0; i < insert_num * thread_number; i++) {
    ASSERT_EQ(l->Contain(i), false);
  }
}

int main() {
  testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}