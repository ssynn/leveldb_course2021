#include <iostream>
#include <time.h>
#include <thread>

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/dumpfile.h"


namespace leveldb{

class StdoutPrinter : public WritableFile {
 public:
  Status Append(const Slice& data) override {
    fwrite(data.data(), 1, data.size(), stdout);
    return Status::OK();
  }
  Status Close() override { return Status::OK(); }
  Status Flush() override { return Status::OK(); }
  Status Sync() override { return Status::OK(); }
};

void WriteData(){
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  op.display_kv = true;
  if(op.env->FileExists("testdb_read_sstable")){
    op.env->DeleteDir("testdb_read_sstable");
  }
  Status status = DB::Open(op, "testdb_read_sstable", &db);
  assert(status.ok());

  for(int i=0;i<5;i++){
    db->Put(WriteOptions(), "00"+std::to_string(i), "00"+std::to_string(i));
  }
  for(int i=0;i<5;i++){
    db->Delete(WriteOptions(), "00"+std::to_string(i));
  }

  db->CompactRange(nullptr, nullptr);

  delete db;
}
}


int main(){
  leveldb::WriteData();
//  leveldb::StdoutPrinter printer;
//  leveldb::Status s = leveldb::DumpFile(leveldb::Env::Default(), "testdb_read_sstable/000021.ldb", &printer);
//  std::vector<std::vector<std::string>>key_value = {{"1", "2"},{"2","444"}};
//  std::tuple<std::string, std::string, std::string>s;
//  std::cout<<key_value[0][0];

  return 0;
}