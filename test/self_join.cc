#include <cassert>
#include <dirent.h>
//#include <format.h>
#include <iostream>
#include <string>
#include <stdio.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/time.h>
#include <ctime>
#include <table/filter_block.h>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "leveldb/iterator.h"
#include "leveldb/slice.h"

#include "db/filename.h"
#include "db/version_set.h"



std::string Trim(std::string& str)
{
    str.erase(0,str.find_first_not_of(" \t\r\n"));
    str.erase(str.find_last_not_of(" \t\r\n") + 1);
    return str;
}

// REQUIRES: min1<=max1, min2<=max2
bool Intersect(int min1, int max1, int min2, int max2)
{
    return (min1>=min2 && min1<=max2) || (max1>=min2 && max1<=max2) || (min1<=min2 && max1>=max2);
}


leveldb::Status LoadData(std::string db_name, std::string file_name)
{
    leveldb::DB *db;
    leveldb::Options options;
    options.comparator = leveldb::NumComparator();
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.create_if_missing=true;
    options.compression = leveldb::kNoCompression;
    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    assert(status.ok());

    std::ifstream fin(file_name,std::ios::in);
    assert(fin.is_open());
    std::string line;
    while (getline(fin, line))
    {

        std::istringstream sin(line);
        std::vector<std::string> fields;
        std::string field;
        while (getline(sin, field, ','))
        {
            fields.push_back(field);
        }
        std::string key = Trim(fields[0]);
        std::string value = Trim(fields[1]);
        status=db->Put(leveldb::WriteOptions(),key,value);
        assert(status.ok());
    }
    fin.close();
    delete db;
    return status;
}

leveldb::Status PrintTablesRange(leveldb::DB *db)
{
    leveldb::Version *current = db->GetCurrentVersion();
    for(int i = 0; i< 7;i++){
        std::vector<leveldb::FileMetaData *> files_ = current->GetFiles(i);
        //
        if (!files_.empty()) {
            for (int i = 0; i < files_.size(); i++) {
                std::cout << files_[i]->number << " ";
                std::cout << files_[i]->smallest.user_key().ToString() << " ";
                std::cout << files_[i]->largest.user_key().ToString() << std::endl;
            }
        }
    }
}
leveldb::Status GetLookupKey(std::vector<std::string>input,
                                std::vector<leveldb::LookupKey*>&result,
                                leveldb::SequenceNumber number)
{
    if(input.empty())
        return leveldb::Status::EmptyInput("");
    for(int i=0;i<input.size();i++)
    {
        leveldb::LookupKey* lky_p = new leveldb::LookupKey(input[i], number);
        result.emplace_back(lky_p);
    }
    return leveldb::Status::OK();
}

leveldb::Status TableFilter(
        leveldb::DB *db, std::string &db_name,
        leveldb::Options options,
        int lower, int upper,
        std::vector<leveldb::LookupKey*>&emails_lk,
        std::vector<leveldb::FileMetaData*>& FileMetaDatas)
{
    if(upper<lower)
        return leveldb::Status::InvalidIdRange("min:"+std::to_string(lower),
                "max:"+std::to_string(upper));
    // table id filter
    leveldb::Version *current = db->GetCurrentVersion();
    for(int level = 0; level< leveldb::config::kNumLevels;level++){
        std::vector<leveldb::FileMetaData *> files_ = current->GetFiles(level);
        if (!files_.empty()) {
            for(auto f:files_) {
                // id filter
                int id_min = atoi(f->smallest.user_key().ToString().c_str());
                int id_max = atoi(f->largest.user_key().ToString().c_str());
                if(!Intersect(lower,upper,id_min,id_max))
                    continue;
                // email filter
                if(emails_lk.empty())
                {
                    FileMetaDatas.emplace_back(f);
                    std::cout<<"warning:no emails filter input" <<std::endl;
                }
                else
                {
                    leveldb::Status status;
                    leveldb::RandomAccessFile *file = nullptr;
                    leveldb::Table *table;
                    status = options.env->NewRandomAccessFile(leveldb::TableFileName(db_name, f->number), &file);
                    assert(status.ok());
                    status = leveldb::Table::Open(options, file, f->file_size, &table);
                    assert(status.ok());
                    //需要构造成internal_key
                    for(auto lk:emails_lk)
                    {
                        if(table->KeyMayMatch(lk->internal_key()))
                        {
                            FileMetaDatas.push_back(f);
                            continue;
                        }

                    }
                }
            }
        }
    }
    return leveldb::Status::OK();
}

leveldb::Status EntryFilter(std::string &db_name,
                        leveldb::Options options,
                        int lower, int upper,
                        std::vector<leveldb::LookupKey*>emails,
                        std::vector<leveldb::FileMetaData*>fileMetaDatas,
                        std::multimap<std::string,std::string> &mmap)
{
    if(fileMetaDatas.empty())
    {
        std::string range = std::to_string(lower)+"-"+std::to_string(upper);
        return leveldb::Status::NoTableHit(range);
    }
    for(auto target_file:fileMetaDatas)
    {
        leveldb::Status status;
        leveldb::RandomAccessFile *file = nullptr;
        leveldb::Table *table;
        status = options.env->NewRandomAccessFile(leveldb::TableFileName(db_name, target_file->number), &file);
        assert(status.ok());
        status = leveldb::Table::Open(options, file, target_file->file_size, &table);
        assert(status.ok());
        //读取符合条件的entry压入mmp
        //Warning:注意判断key已删除和重复的情况
        leveldb::Iterator *table_iter = table->NewIterator(leveldb::ReadOptions());
        //TODO:确定起点和终点而不是全部遍历
        //TODO:没考虑历史数据的情况
//        int min_id = atoi(target_file->smallest.user_key().ToString().c_str());
//        int max_id = atoi(target_file->largest.user_key().ToString().c_str());
//        if(min_id<lower) {
//            //起点为lower
//        }else {
//            //起点为min_id
//        }
//        if(max_id>upper) {
//            //终点为upper
//        } else {
//            //终点为max_id
//        }
        table_iter->SeekToFirst();
        while (table_iter->Valid()) {
            leveldb::ParsedInternalKey ikey;
            leveldb::ParseInternalKey(table_iter->key(), &ikey);
            if(ikey.type!=leveldb::kTypeDeletion){
                std::string key = ikey.user_key.ToString();
                std::string value = table_iter->value().ToString();
                int key_int = std::atoi(key.c_str());
                if(key_int>=lower&&key_int<=upper)
                {
                    for(auto email_lky:emails)
                    {
                        if(value.compare(email_lky->user_key().ToString())==0)
                            mmap.insert(std::make_pair(value,key));
                    }
                }
            }
            table_iter->Next();
        }
    }
    return leveldb::Status::OK();
}

int main()
{
    timeval start, end;
    std::string db_name = "/home/honwee/CLionProjects/courseForLeveldb/test/mydb";
    std::string data_file = "/home/honwee/CLionProjects/courseForLeveldb/test/data/TestData50w.csv";
//    leveldb::Status s = LoadData(db_name, data_file);

    leveldb::DB *db;
    leveldb::Options options;
    options.comparator = leveldb::NumComparator();
    options.filter_policy = leveldb::NewBloomFilterPolicy(10);
    options.create_if_missing=true;
    options.compression = leveldb::kNoCompression;
    leveldb::Status status = leveldb::DB::Open(options, db_name, &db);
    assert(status.ok());
//    PrintTablesRange(db);

    int lower = 4;
    int upper = 30000;
    std::vector<std::string> emails;
    emails.emplace_back("XZS@ecnu.cn");
    std::vector<leveldb::LookupKey*> emails_lkey;
    leveldb::SequenceNumber snapshot = db->GetLastSequenceNumber();
    status = GetLookupKey(emails, emails_lkey, snapshot);
    assert(status.ok());

    std::vector<leveldb::FileMetaData*> fileMetaDatas;
    gettimeofday(&start, nullptr);
    status = TableFilter(db,db_name,options,lower,upper,emails_lkey,fileMetaDatas);
    assert(status.ok());
    std::cout << "table hit:" << fileMetaDatas.size() << std::endl;

    std::multimap<std::string,std::string> mmap;
    status = EntryFilter(db_name,options,lower,upper,emails_lkey,fileMetaDatas,mmap);
    assert(status.ok());
    gettimeofday(&end, nullptr);

    std::string email = "XZS@ecnu.cn";
    std::multimap<std::string, std::string>::iterator it = mmap.find(email);
    if(it !=mmap.end())
    {
        for(unsigned int i = 0; i < mmap.count(email); ++i){
            std::cout<<it->second<<std::endl;
            ++it;
        }
    }

    std::cout<<"time total:" << end.tv_sec-start.tv_sec<<"s,"<<end.tv_usec-start.tv_usec<<"us"<<std::endl;

    for(auto email_p : emails_lkey)
        delete email_p;
    delete db;
}

