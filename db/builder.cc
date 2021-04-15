// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <iostream>
#include "db/builder.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {


std::string DecoderFromKV(const Slice& key, const Slice& value){
  std::string ans = "'";
  // 提取Key
  ans += std::string(key.data(), key.size()-8) + "' @ ";

  // 提取Version
  const uint64_t tag = DecodeFixed64(key.data()+key.size()-8);
  uint64_t ver = tag >> 8;
  ans += std::to_string(ver);

  // 提取value
  switch (tag & 0xff) {
    case 1: {
      ans += " : val => '" + std::string(value.data(), value.size()) + "'\n";
      break;
    }
    case 0:
      ans += " : del => ''\n";
  }
  return ans;
}


Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);

  std::vector<std::string>key_value;

  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    for (; iter->Valid(); iter->Next()) {
      Slice key = iter->key();
      meta->largest.DecodeFrom(key);//记录当前SSTable最大的Key
      builder->Add(key, iter->value());
      key_value.push_back(DecoderFromKV(key, iter->value())); // NOTE 将KV记录
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }

    //NOTE 写入明文
    if(options.display_kv){
      WritableFile* plaintext_file;
      std::string plaintext_file_name = PlainTextFileName(dbname, meta->number);
      env->NewWritableFile(plaintext_file_name, &plaintext_file);
      std::string meta_str;
      meta_str += "number: " + std::to_string(meta->number);
      meta_str += "\nfile_size: " + std::to_string(meta->file_size);
      meta_str += "\nsmallest: " + std::string(meta->smallest.user_key().data(), meta->smallest.user_key().size());
      meta_str += "\nlargest: " + std::string(meta->largest.user_key().data(), meta->largest.user_key().size());
      meta_str += "\n";
      plaintext_file->Append(meta_str.data());
      for(auto &i:key_value){
        plaintext_file->Append(i.data());
      }
      plaintext_file->Flush();
      plaintext_file->Close();
    }

    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
