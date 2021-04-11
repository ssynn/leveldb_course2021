
#define SHOW

#include <iostream>
#include <vector>

#include "gtest/gtest.h"

struct ReadOption {
  int version;
  ReadOption() { version = -1; }
};

enum Op { Value = 0, Delete = 1 };

class List {
 public:
  struct Node;

  List();

  bool Put(int key, int value);

  bool Delete(int key);

  bool Get(ReadOption r, int key, int& value);

  int GetSnapshot();

  std::string ToString();

 private:
  Node* header;
};

struct List::Node {
  Op op;
  int key;
  int value;
  int version;
  Node* next;

  Node() {
    key = 0;
    value = 0;
    version = 0;
    next = nullptr;
  }

  explicit Node(int k, int v, int version, Op op)
      : key(k), value(v), version(version), next(nullptr), op(op) {}

  // 如果key不相同，则按照key从小到大排序
  // 如果key相同，则按照version从大到小排序
  bool operator<(const Node& b) const {
    if (key < b.key) return true;
    if (key == b.key && version > b.version) return true;
    return false;
  }
};

List::List() { header = new Node(); }

// TODO: 插入一个新节点，版本号为最新版本
bool List::Put(int key, int value) {
  // 创建要插入的节点
  Node* t = new Node(key, value, header->version + 1, Value);

  // 找到第一个大于等于t节点
  Node* p = header;
  while (p->next != nullptr && *(p->next) < *t) {
    p = p->next;
  }

  // 插入节点
  t->next = p->next;
  p->next = t;

  header->version++;
  return true;
}

// TODO: ReadOption内传入版本号，
// 返回值存在value，如果没有找到对应的节点则返回false
bool List::Get(ReadOption r, int key, int& value) {
  // 获取当前最新版本号，如果WriteOption内传入版本号不为-1则使用传入的版本号
  int _version = r.version;
  if (_version < 0) _version = header->version;

  // 创建一个节点用于和链表内节点比较
  Node* t = new Node(key, 0, _version, Value);

  // 找到第一个大于等于t节点
  Node* p = header;
  while (p->next != nullptr && *(p->next) < *t) {
    p = p->next;
  }

  // 如果查到的是标记为删除的节点则没有查找
  if (!p->next || p->next->key != key || p->next->op == Op::Delete) return false;
  value = p->next->value;

  delete t;
  return true;
}

// TODO: 插入一个操作为删除的节点
bool List::Delete(int key) {
  // 与Put唯一的区别就是操作为Delete
  Node* t = new Node(key, 0, header->version + 1, Op::Delete);
  Node* p = header;
  while (p->next && *(p->next) < *t) {
    p = p->next;
  }
  t->next = p->next;
  p->next = t;
  header->version++;
  return true;
}

// TODO: 返回当前的版本号
int List::GetSnapshot() { return header->version; }

std::string List::ToString() {
  std::string ans;
  Node* p = header->next;
  while (p != nullptr) {
    if (p->op == Value) {
      ans += "Key: " + std::to_string(p->key) +
             ", Version: " + std::to_string(p->version) +
             ", Value: " + std::to_string(p->value) + "\n";
    } else {
      ans += "Key: " + std::to_string(p->key) +
             ", Version: " + std::to_string(p->version) + ", Delete\n";
    }
    p = p->next;
  }
  ans += "------------------------PRINT------------------------\n";
  return ans;
}

TEST(Node, Compare) {
  auto* c = new List::Node(-2, -2, 3, Value);
  auto* d = new List::Node(1, 11, 4, Delete);
  auto* a = new List::Node(1, 1, 1, Value);
  auto* b = new List::Node(2, 2, 2, Value);

  ASSERT_LT(*a, *b);
  ASSERT_LT(*d, *a);
  ASSERT_LT(*c, *a);
}

TEST(List, PutAndGet) {
  List* l = new List();
  ASSERT_TRUE(l->Put(1, 1));
  ASSERT_TRUE(l->Put(2, 2));
  ASSERT_TRUE(l->Put(-2, -2));
  ASSERT_TRUE(l->Put(23, 2323));

#ifdef SHOW
  std::cout<<l->ToString();
#endif

  int value;
  ASSERT_TRUE(l->Get(ReadOption(), 1, value));
  ASSERT_EQ(value, 1);

  ASSERT_TRUE(l->Get(ReadOption(), 2, value));
  ASSERT_EQ(value, 2);

  ASSERT_TRUE(l->Get(ReadOption(), -2, value));
  ASSERT_EQ(value, -2);

  ASSERT_TRUE(l->Get(ReadOption(), 23, value));
  ASSERT_EQ(value, 2323);

  // 这里覆盖Key=1的数据，读取出来的value应该是6
  ASSERT_TRUE(l->Put(2, 6));
  ASSERT_TRUE(l->Get(ReadOption(), 2, value));
  ASSERT_EQ(value, 6);

  // 查找一个不存在的数据，返回false
  ASSERT_FALSE(l->Get(ReadOption(), 20000, value));

#ifdef SHOW
  std::cout<<l->ToString();
#endif
  delete l;
}

TEST(List, Delete) {
  List* l = new List();

  l->Put(1, 1);
  l->Put(2, 2);
  l->Put(4, 4);
  l->Put(-2, -2);
  l->Put(12, 12);
  l->Put(-12, -12);
#ifdef SHOW
  std::cout<<l->ToString();
#endif

  l->Delete(1);
  l->Delete(2);
  l->Delete(4);
  l->Delete(-12);

  int ans;
  ASSERT_FALSE(l->Get(ReadOption(), 1, ans));
  ASSERT_FALSE(l->Get(ReadOption(), 2, ans));
  ASSERT_FALSE(l->Get(ReadOption(), 4, ans));
  ASSERT_FALSE(l->Get(ReadOption(), -12, ans));

  ASSERT_TRUE(l->Get(ReadOption(), -2, ans));
  ASSERT_TRUE(l->Get(ReadOption(), 12, ans));
#ifdef SHOW
  std::cout<<l->ToString();
#endif

  delete l;
}

// 这里复现实验一的场景
TEST(List, Snapshot) {
  List* l = new List();
  int ans;

  int value_old[10] = {85, 90, 75, 60, 73, 86, 68, 88, 85, 94};
  int value_new[10] = {80, 91, 80, 80, 70, 89, 70, 90, 88, 95};


  // 把value_old数据插入，Key为0-9，并创建快照s1
  for(int i=0;i<10;i++){
    l->Put(i, value_old[i]);
  }

  ReadOption s1;
  s1.version = l->GetSnapshot();

  // 用value_new覆盖value_old的数据，创建快照s2
  for(int i=0;i<10;i++){
    l->Put(i, value_new[i]);
  }

  ReadOption s2;
  s2.version = l->GetSnapshot();

  // 删除所有的数据
  for(int i=0;i<10;i++){
    l->Delete(i);
  }

  // 通过s1版本号读取出的数据依然是s1版本的数据
  for(int i=0;i<10;i++){
    ASSERT_TRUE(l->Get(s1, i, ans));
    ASSERT_EQ(ans, value_old[i]);
  }

  // 通过s2版本号读取出的数据依然是s2版本的数据
  for(int i=0;i<10;i++){
    ASSERT_TRUE(l->Get(s2, i, ans));
    ASSERT_EQ(ans, value_new[i]);
  }

  // 这里Get之前删除的数据，查找失败
  for(int i=0;i<10;i++){
    ASSERT_FALSE(l->Get(ReadOption(), i, ans));
  }
#ifdef SHOW
  std::cout<<l->ToString();
#endif
}

int main() {
  testing::InitGoogleTest();
  return RUN_ALL_TESTS();
}