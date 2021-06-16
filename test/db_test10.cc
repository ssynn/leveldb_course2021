#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "sys/time.h"
#include "map"
#include "unordered_map"

#define SHOW_DATA false

using namespace leveldb;

static inline int64_t GetUnixTimeUs() {
    struct timeval tp;
    gettimeofday(&tp, nullptr);
    return (((int64_t) tp.tv_sec) * 1000000 + (int64_t) tp.tv_usec);
}

// 这里测试当字符串被释放掉后，下次传入同样的字符串是否能找到
TEST(unordered_map, insert_and_get){
  std::unordered_map<const char*, int>ump;

#if 0
  // 测试同地址不同数据
  char* b = new char[10]();
  for(int i=0;i<10;i++){
    memset(b, 0, 10);
    b[0] = 'a'+i;
    b[1] = 'a'+i;
    b[3] = 'a'+i;
    ump[b] = i;
  }

  for(int i=0;i<10;i++){
    memset(b, 0, 10);
    b[0] = 'a'+i;
    b[1] = 'a'+i;
    b[3] = 'a'+i;
    ASSERT_EQ(ump[b], i);
  }
  // 结论，unordered_map不会复制一份const char*的数据
#endif

#if 0
  // 测试不同地址相同数据
  std::unordered_map<const char*, int>ump2;
  char* a0 = new char[10]();
  char* a1 = new char[10]();
  char* a2 = new char[10]();

  for(int i=0;i<3;i++){
    a0[i] = 'a';
    a1[i] = 'a';
    a2[i] = 'a';
  }
  std::cout<<a0<<" "<<a1<<" "<<a2<<"\n";
  ASSERT_EQ(*a0, *a1);
  ASSERT_EQ(*a1, *a2);
  ump2[a0] = 11;
  ASSERT_EQ(ump2[a0], ump2[a1]);
  ASSERT_EQ(ump2[a1], ump2[a2]);
  // 还是不行呀
#endif

  // 算了还是用string吧
  std::unordered_map<std::string, int>ump3;
  char* a0 = new char[10]();
  char* a1 = new char[10]();
  char* a2 = new char[10]();

  for(int i=0;i<3;i++){
    a0[i] = 'a';
    a1[i] = 'a';
    a2[i] = 'a';
  }
  // std::cout<<a0<<" "<<a1<<" "<<a2<<"\n";
  std::string b0(a0, 3);
  std::string b1(a1, 3);
  std::string b2(a2, 3);
  delete []a0;
  delete []a1;
  delete []a2;
  ASSERT_EQ(b0, b1);
  ASSERT_EQ(b1, b2);
  ump3[b0] = 11;
  ASSERT_EQ(ump3[b0], ump3[b1]);
  ASSERT_EQ(ump3[b1], ump3[b2]);
  // 还是string靠谱
}

#if 0
TEST(Map, unordered_mapVSmap){
  std::unordered_map<const char*, int>ump;
  std::map<const char*, int>mp;
  for (size_t i = 0; i < 10000; i++)
  {
    ump[std::to_string(i).c_str()] = i;
  }

  for (size_t i = 0; i < 10000; i++)
  {
    mp[std::to_string(i).c_str()] = i;
  }

    int size = 4000*512;
    uint64_t* data = new uint64_t[size]();
    uint64_t start = GetUnixTimeUs();
    for (size_t i = 0; i < size; i++)
    {
      data[i] &= 1;
    }
    std::cout<<"unordered_map"<<GetUnixTimeUs()-start<<"us\n";

  start = GetUnixTimeUs();
  for (size_t i = 0; i < 10000; i++)
  {
    mp.find(std::to_string(i).c_str());
  }
  std::cout<<"map"<<GetUnixTimeUs()-start<<"us\n";

}
#endif


TEST(Memtable, get_with_hashmap){
  DB *db = nullptr;
  Options op;
  op.write_buffer_size = 1<<24;
  op.create_if_missing = true;
  op.with_hashmap = true;
  Status status = DB::Open(op, "testdb_memtable", &db);
  assert(status.ok());

  int length = 100000;

  for(int i=0;i<length;i++){
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }

  uint64_t start = GetUnixTimeUs();
  std::string value;
  for(int i=0;i<length;i++){
    status = db->Get(ReadOptions(), std::to_string(i), &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, std::to_string(i));
  }
  std::cout<<"Read with index: "<<GetUnixTimeUs()-start<<"us\n";
}

TEST(Memtable, get_without_hashmap){
  DB *db = nullptr;
  Options op;
  op.write_buffer_size = 1<<24;
  op.create_if_missing = true;
  op.with_hashmap = false;
  Status status = DB::Open(op, "testdb_memtable_noidx", &db);
  assert(status.ok());

  int length = 100000;

  for(int i=0;i<length;i++){
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }

  uint64_t start = GetUnixTimeUs();
  std::string value;
  for(int i=0;i<length;i++){
    status = db->Get(ReadOptions(), std::to_string(i), &value);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(value, std::to_string(i));
  }
  std::cout<<"Read without index: "<<GetUnixTimeUs()-start<<"us\n";
}

TEST(ColumnFamily, read){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb_cf", &db);
  assert(status.ok());

  ColumnFamilyHandle cf1("test1");
  ColumnFamilyHandle cf2("test2");

  db->Put(WriteOptions(), cf1, "123", "123");
  db->Put(WriteOptions(), cf2, "321", "321");

  std::string value;
  status = db->Get(ReadOptions(), cf1, "321", &value);
  ASSERT_FALSE(status.ok());
  status = db->Get(ReadOptions(), cf2, "321", &value);
  ASSERT_TRUE(status.ok());

#if SHOW_DATA
  std::cout<<value<<"\n";
#endif

  status = db->Get(ReadOptions(), cf2, "123", &value);
  ASSERT_FALSE(status.ok());
  status = db->Get(ReadOptions(), cf1, "123", &value);
  ASSERT_TRUE(status.ok());
#if SHOW_DATA
  std::cout<<value<<"\n";
#endif
}

TEST(ColumnFamily, iterator){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb_cf2", &db);
  assert(status.ok());

  ColumnFamilyHandle cf1("test1");
  ColumnFamilyHandle cf2("test2");

  for(int i=0;i<10;i++){
    db->Put(WriteOptions(), cf1, std::to_string(i), std::to_string(i));
  }
  for(int i=10;i<20;i++){
    db->Put(WriteOptions(), cf2, std::to_string(i), std::to_string(i));
  }

  Iterator* iter1 = db->NewColumnFamilyIterator(ReadOptions(), cf1);
  iter1->SeekToFirst();
  ASSERT_EQ(Slice("0").ToString(), iter1->key().ToString());

#if SHOW_DATA
  for(;iter1->Valid();iter1->Next()){
    std::cout<<iter1->key().ToString()<<","<<iter1->value().ToString()<<" ";
  }
  std::cout<<std::endl;
#endif
  delete iter1;

  Iterator* iter2 = db->NewColumnFamilyIterator(ReadOptions(), cf2);
  iter2->SeekToFirst();
  ASSERT_EQ(Slice("10"), iter2->key());

#if SHOW_DATA
  for(;iter2->Valid();iter2->Next()){
    std::cout<<iter2->key().ToString()<<","<<iter2->value().ToString()<<" ";
  }
  std::cout<<std::endl;
#endif
  delete iter2;

  Iterator* it = db->NewIterator(ReadOptions());
  it->SeekToFirst();
  ASSERT_EQ(Slice("test1_0").ToString(), it->key().ToString());

#if SHOW_DATA
  for(;it->Valid();it->Next()){
    std::cout<<it->key().ToString()<<","<<it->value().ToString()<<" ";
  }
#endif
  delete it;
  delete db;
}

TEST(Index, PutWithIndex){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb_idx", &db);
  assert(status.ok());

  ColumnFamilyHandle cf_record("Record");
  ColumnFamilyHandle cf_index("Index");

  db->PutWithIndex(WriteOptions(), "1", "111");

  std::string value;
  Status s = db->Get(ReadOptions(), cf_record, "1", &value);
  ASSERT_FALSE(s.ok());
  s = db->Get(ReadOptions(), cf_record, "00000001", &value);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ("00000111", value);
  s = db->Get(ReadOptions(), cf_index, "00000111_00000001", &value);
  ASSERT_TRUE(s.ok());
}

TEST(Index, GetIntPos){
  ASSERT_EQ(IndexIterator::GetIntPos("0000", 4), 3);
  ASSERT_EQ(IndexIterator::GetIntPos("100", 3), 0);
  ASSERT_EQ(IndexIterator::GetIntPos("00000100", 8), 5);
}

TEST(Index, Iterator){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb_idx_iter", &db);
  assert(status.ok());
  for (int i = 0; i < 10; i++)
  {
    db->PutWithIndex(WriteOptions(), std::to_string(i), std::to_string(100-i));
  }

  Iterator* idx_iter = db->NewIndexIterator(ReadOptions());
  idx_iter->SeekToFirst();
  for(int i=0;i<10;i++){
  #if SHOW_DATA
    std::cout<<idx_iter->key().ToString()<<","<<idx_iter->value().ToString()<<" ";
  #endif
    // ASSERT_GT(last, idx_iter->value().ToString());
    // last = idx_iter->value().ToString();
    ASSERT_EQ(idx_iter->key().ToString(), std::to_string(91+i));
    ASSERT_EQ(idx_iter->value().ToString(), std::to_string(9-i));
    idx_iter->Next();
  }
  delete idx_iter;
  delete db;
}

// #define READ_GRADE

#ifdef READ_GRADE
TEST(Index, GetGrade){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "exp_grade", &db);
  assert(status.ok());

  int i = 0;

  char key[9];
  char value[9];
  int a, b;
  freopen("../test/data.csv", "r", stdin);
  uint64_t start = GetUnixTimeUs();
  while(~scanf("%d,%d\n", &a, &b)){
    i++;
#if false
    std::cout<<a<<" "<<b<<"\n";
    if(i>9){
      break;
    }
#endif
    db->PutWithIndex(WriteOptions(), std::to_string(a), std::to_string(b));
  }
  std::cout<<i<<" read finished in "<<GetUnixTimeUs()-start<<"us\n";
  fclose(stdin);
  sleep(2);
}
#endif

#ifndef READ_GRADE
TEST(Index, scan){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "exp_grade", &db);
  assert(status.ok());
  
  std::string start = "600";
  std::string end = "700";

  Iterator* iter = db->NewIndexIterator(ReadOptions());
  iter->Seek(start);
  ASSERT_EQ(iter->key().ToString(), "600");
  ASSERT_EQ(iter->value().ToString(), "111");
#if SHOW_DATA
  std::cout<<iter->key().ToString()<<" "<<iter->value().ToString()<<"\n";
#endif

  int cnt = 0;
  uint64_t iter_start = GetUnixTimeUs();
  for(;iter->Valid();iter->Next()){
    if(iter->key().ToString()>end){
      break;
    }
    cnt++;
  }
  delete iter;
  ASSERT_EQ(cnt, 663755);
  std::cout<<cnt<<" Iteration with index finished in "<<GetUnixTimeUs()-iter_start<<"us\n";

  cnt = 0;
  iter = db->NewColumnFamilyIterator(ReadOptions(), *(new ColumnFamilyHandle("Record")));
  iter->SeekToFirst();
  ASSERT_EQ(iter->key().ToString(), "00000001");
  iter_start = GetUnixTimeUs();
  for(;iter->Valid();iter->Next()){
    if(iter->value().ToString()<="00000700" && iter->value().ToString()>="00000600"){
      cnt++;
    }
  }
  ASSERT_EQ(cnt, 663755);
  std::cout<<cnt<<" Iteration without index finished in "<<GetUnixTimeUs()-iter_start<<"us\n";
}
#endif

int main() {
 system("rm -rf ./testdb*");
 usleep(100);
 testing::InitGoogleTest();
 return RUN_ALL_TESTS();
}