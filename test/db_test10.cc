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

TEST(Memtable, get){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb_read_sstable", &db);
  assert(status.ok());

  for(int i=0;i<50000;i++){
    db->Put(WriteOptions(), std::to_string(i), std::to_string(i));
  }
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

#ifdef SHOW_DATA
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


//  std::unordered_map<const char*, int>ump;
//  std::map<const char*, int>mp;
//
//  for (size_t i = 0; i < 10000; i++)
//  {
//    ump[std::to_string(i).c_str()] = i;
//  }
//
//  for (size_t i = 0; i < 10000; i++)
//  {
//    mp[std::to_string(i).c_str()] = i;
//  }
//
  // int size = 4000*512;
  // uint64_t* data = new uint64_t[size]();
  // uint64_t start = GetUnixTimeUs();
  // for (size_t i = 0; i < size; i++)
  // {
  //   data[i] &= 1;
  //   //  ump.find(std::to_string(i).c_str());
  // }
  // std::cout<<"unordered_map"<<GetUnixTimeUs()-start<<"us\n";
//
//  start = GetUnixTimeUs();
//  for (size_t i = 0; i < 10000; i++)
//  {
//    mp.find(std::to_string(i).c_str());
//  }
//  std::cout<<"map"<<GetUnixTimeUs()-start<<"us\n";
//
//  return 0;
}