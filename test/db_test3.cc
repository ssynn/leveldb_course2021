#include "leveldb/db.h"
#include <cstdio>
#include <iostream>
#include "db/skiplist.h"

using namespace std;
using namespace leveldb;
int main() {
  cout<<"ok\n";
  DB *db = nullptr;
  Options op;
  op.create_if_missing = true;
  Status status = DB::Open(op, "testdb", &db);
  assert(status.ok());

  freopen("../test/input1", "r", stdin);
  string key, value;
  for(int i=0; i<10; i++){
    cin >> key >> value;
    db->Put(WriteOptions(), key, value);
  }
  fclose(stdin);

  ReadOptions op1;
  op1.snapshot = db->GetSnapshot();

  cin.clear();
  freopen("../test/input2", "r", stdin);
  for(int i=0; i<10; i++){
    cin >> key >> value;
    db->Put(WriteOptions(), key, value);
  }
  fclose(stdin);

  freopen("../test/output2", "w", stdout);
  string s[15];
  int i = 0;
  Iterator* iter = db->NewIterator(op1);
  for(iter->SeekToFirst(); iter->Valid(); iter->Next()){
    s[i] = iter->key().ToString() + ' ' + iter->value().ToString() + ' ';
    i ++;
  }

  Iterator* iter1 = db->NewIterator(ReadOptions());
  i = 0;
  for(iter1->SeekToFirst(); iter1->Valid(); iter1->Next()){
    s[i] += iter1->value().ToString();
    cout << s[i] << endl;
    i ++;
  }
  fclose(stdout);

  delete iter;
  delete iter1;
  db->ReleaseSnapshot(op1.snapshot);
  return 0;
}
