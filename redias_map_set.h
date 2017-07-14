/*
 *  redias_map_set.h
 *
 *  Created on: 2016Äê5ÔÂ25ÈÕ
 *      Author: root
 */

#ifndef REDIS_MAP_SET_H_
#define REDIS_MAP_SET_H_

#include "redis_map_set_base.h"


class redis_map_set :public redis_map_set_base {
public:
    
	template<typename NameType>
    redis_map_set(int db, NameType name, redisMgr *pRedisMgr = NULL, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *bscSplit = ","):
	    redis_map_set_base(pRedisMgr, run_cmd_Mod, syncChannel, syncdb, basic_sync_cmd, bscSplit),
	    m_name(name),
	    m_db(db) 
	{	
	     stringstream ss;
	     ss<<name;
	     m_name.assign(ss.str());   
	}
	
	redis_map_set():
	    m_db(0)
	{
	}
	
	~redis_map_set()
	{
	}
	
	template<typename NameType>
	bool init(int db, NameType &name, redisMgr *pRedisMgr = NULL, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *bscSplit = ",")
    {
        m_db = db;
        stringstream ss;
	    ss<<name;
	    m_name.assign(ss.str());   
	    return redis_map_set_base::init(pRedisMgr, run_cmd_Mod, syncChannel, syncdb, basic_sync_cmd, bscSplit);
	}
		 
	void init()
	{
	     return redis_map_set_base::init();
	}
	
	template<typename KeyType, typename ValType>
    bool insert(KeyType &key, ValType &val)
    {
        string tmp_key;
        tmp_key = m_name + bitTostring(key);      
        string tmp_val;
        bitTostring(val, tmp_val);
         
        return redis_map_set_base::insert(tmp_key, tmp_val, m_db);     
    }
    
    template<typename KeyType, typename ValType> 
    bool find(KeyType &key, vector<ValType> &val)
    {
        string tmp_key;
        tmp_key = m_name + bitTostring(key);    
        
        vector<string> tmp_vals;
        if (!redis_map_set_base::find(tmp_key, tmp_vals, m_db) )
            return false;        
        if (tmp_vals.empty())
            return false;
        vector<ValType> res(tmp_vals.size(), ValType());
        for (int index = 0; index < tmp_vals.size(); ++index)
        {
           res[index] = tmp_vals[index]; 
        }
        val.swap(res);
        return true;
    }
    
    template<typename KeyType, typename ValType>
    int count(KeyType &key, ValType &val)
    {
        string tmp_key;
        tmp_key = m_name + bitTostring(key);      
        string tmp_val;
        bitTostring(val, tmp_val);
         
        return redis_map_set_base::count(tmp_key, tmp_val, m_db);     
    }
    
    template<typename KeyType, typename ValType>    
    bool erase(KeyType &key, ValType &val)
    {
        string tmp_key;
        tmp_key = m_name + bitTostring(key);      
        string tmp_val;
        bitTostring(val, tmp_val);
        return redis_map_set_base::erase(tmp_key, tmp_val, m_db);
    }
    
    template<typename KeyType> 
    bool erase(KeyType &key)
    {
        string tmp_key;
        tmp_key = m_name + bitTostring(key);          
        vector<string> tmp_vals;
        redis_map_set_base::find(tmp_key, tmp_vals, m_db);
        if (tmp_vals.empty())
            return false;
        for (int index = 0; index < tmp_vals.size(); ++index)
        {
           if(!redis_map_set_base::erase(tmp_key, tmp_vals[index], m_db))
            return false;
        }
        return true;        
    }
    
    bool clean()
    {
        vector<string> keys;
        string filter(m_name);
        filter.append("*");
        redis_scan_keys(filter, keys, m_db);
        for (int kindex = 0; kindex < keys.size(); ++kindex) 
        {
            vector<string> tmp_vals;
            redis_map_set_base::find(keys[kindex], tmp_vals, m_db);
            if (tmp_vals.empty())
                return false;
            for (int index = 0; index < tmp_vals.size(); ++index)
            {
                if(!redis_map_set_base::erase(keys[kindex], tmp_vals[index], m_db))
                    return false;
            }
        }      
    }
       
private:
    string m_name;
    int m_db;        
};

#endif /* REDIS_MAP_SET_H_ */
