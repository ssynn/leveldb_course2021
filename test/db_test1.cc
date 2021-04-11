#include <cstdio>
#include <iostream>

#include "leveldb/db.h"

using namespace std;
using namespace leveldb;

int main() {
  DB* db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb2", &db);
  assert(status.ok());
  status = db->Put(WriteOptions(), "1", "hobo");
  assert(status.ok());
  status = db->Put(WriteOptions(), "2", "hobo2");
  assert(status.ok());
  status = db->Put(WriteOptions(), "1", "hobo3");
  assert(status.ok());
  status = db->Delete(WriteOptions(), "1");
  assert(status.ok());

  delete db;
  return 0;
}
