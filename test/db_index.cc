//
// Created by rui on 19-5-29.
//

#include "leveldb/db.h"
#include <cstdio>
#include <iostream>
#include <include/leveldb/write_batch.h>
#include <fstream>
#include <sstream>
#include <map>


using namespace std;
using namespace leveldb;

string Trim(string& str)
{
  //str.find_first_not_of(" \t\r\n"),在字符串str中从索引0开始，返回首次不匹配"\t\r\n"的位置
  str.erase(0,str.find_first_not_of(" \t\r\n"));
  str.erase(str.find_last_not_of(" \t\r\n") + 1);
  return str;
}


int main(){
  //opening a database
  leveldb::DB *db;
  leveldb::Options options;
  options.create_if_missing=true;
  options.compression = kNoCompression;
  leveldb::Status status=leveldb::DB::Open(options,"mydbindex",&db);
  assert(status.ok());

  ifstream fin("../test/text.csv");
  string line;
  while (getline(fin, line))
  {

    istringstream sin(line);
    vector<string> fields;
    string field;
    while (getline(sin, field, ','))
    {
      fields.push_back(field);
    }
    string key = Trim(fields[0]);
    string value = Trim(fields[1]);
    status=db->Put(leveldb::WriteOptions(),key,value);
    assert(status.ok());
  }

  std::multimap<std::string,std::string> mmap;
  vector<std::string> val;


  //iterator
  leveldb::Iterator *iter=db->NewIterator(leveldb::ReadOptions());
  for(iter->SeekToFirst();iter->Valid();iter->Next())
  {
    //std::cout<<iter->key().ToString()<<":"<<iter->value().ToString()<<std::endl;
    mmap.insert(std::make_pair(iter->value().ToString(),iter->key().ToString()));
  }


//  std::string strFind = "XUR@ecnu.cn";
  std::string strFind = "c";
  std::multimap<std::string, std::string>::iterator it = mmap.find(strFind);
//  if (it != mmap.end())
//  {
//    for (unsigned int i = 0; i < mmap.count(strFind); ++i)
//    {
//      val.push_back((*it).second);
//      it++;
//    }
//  }

  if(it !=mmap.end())
  {
      for(unsigned int i = 0; i < mmap.count(strFind); ++i){
        cout<<it->second<<endl;
        //val.push_back((*it).second);
        ++it;
      }

  }


  for(int i =0;i < val.size();i++)
    cout << val[i] << " " ;



  delete iter;
  return 0;
}