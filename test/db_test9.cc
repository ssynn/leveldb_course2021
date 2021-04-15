#include <iostream>
#include "leveldb/db.h"

namespace leveldb{
void GetDataWithPosition(){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb_read_sstable", &db);
  assert(status.ok());

  for(int i=0;i<5;i++){
    db->Put(WriteOptions(), "00"+std::to_string(i), "00"+std::to_string(i));
  }
  std::string res;
  std::string pos;
  for(int i=0;i<5;i++){
    Status s = db->GetWithPosition(ReadOptions(),"00"+std::to_string(i), &res, &pos);
    s.ok() ? std::cout<<pos<<std::endl : std::cout<<"err"<<std::endl;
  }

  db->Write(WriteOptions(), nullptr);
  for(int i=0;i<5;i++){
    Status s = db->GetWithPosition(ReadOptions(),"00"+std::to_string(i), &res, &pos);
    s.ok() ? std::cout<<pos<<std::endl : std::cout<<"err"<<std::endl;
  }

  db->CompactRange(nullptr, nullptr);

  for(int i=0;i<5;i++){
    Status s = db->GetWithPosition(ReadOptions(),"00"+std::to_string(i), &res, &pos);
    s.ok() ? std::cout<<pos<<std::endl : std::cout<<"err"<<std::endl;
  }

  for(int i=5;i<10;i++){
    db->Put(WriteOptions(), "00"+std::to_string(i), "00"+std::to_string(i));
  }
  for(int i=5;i<10;i++){
    Status s = db->GetWithPosition(ReadOptions(),"00"+std::to_string(i), &res, &pos);
    s.ok() ? std::cout<<pos<<std::endl : std::cout<<"err"<<std::endl;
  }
  delete db;
}
}


int main(){
  leveldb::GetDataWithPosition();
  return 0;
}