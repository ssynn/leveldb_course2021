#include "gtest/gtest.h"
#include "leveldb/db.h"
#include "sys/time.h"
#include "map"
#include "unordered_map"

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
  // std::cout<<value<<"\n";

  status = db->Get(ReadOptions(), cf2, "123", &value);
  ASSERT_FALSE(status.ok());
  status = db->Get(ReadOptions(), cf1, "123", &value);
  ASSERT_TRUE(status.ok());
  // std::cout<<value<<"\n";
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

  Iterator* iter2 = db->NewColumnFamilyIterator(ReadOptions(), cf2);
  iter2->SeekToFirst();
  ASSERT_EQ(Slice("10"), iter2->key());

  // for(;iter2->Valid();iter2->Next()){
  //   std::cout<<iter2->key().ToString()<<","<<iter2->value().ToString()<<" ";
  // }
  // std::cout<<std::endl;

  Iterator* it = db->NewIterator(ReadOptions());
  it->SeekToFirst();
  ASSERT_EQ(Slice("test1_0").ToString(), it->key().ToString());
  // for(;it->Valid();it->Next()){
  //   std::cout<<it->key().ToString()<<","<<it->value().ToString()<<" ";
  // }
}


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
//  uint64_t start = GetUnixTimeUs();
//  for (size_t i = 0; i < 10000; i++)
//  {
//    ump.find(std::to_string(i).c_str());
//  }
//  std::cout<<"unordered_map"<<GetUnixTimeUs()-start<<"us\n";
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