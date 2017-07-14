/*
 *  redis_map_map.h
 *
 *  Created on: 2016Äê5ÔÂ25ÈÕ
 *      Author: root
 */

#ifndef REDIS_MAP_MAP_H_
#define REDIS_MAP_MAP_H_

#include "redis_map_map_base.h"
	

class redis_map_map :public redis_map_map_base{
public:

	template<typename NameType>
    redis_map_map(int db, NameType name, redisMgr *pRedisMgr = NULL, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *bscSplit = ","):
	    redis_map_map_base(pRedisMgr, run_cmd_Mod, syncChannel, syncdb, basic_sync_cmd, bscSplit),
	    m_name(name),
	    m_db(db) 
	{	    
	}
	
	redis_map_map():
	    m_db(0)
	{
	}
	
	~redis_map_map()
	{
	}
	
	template<typename NameType>
	bool init(int db, NameType &name, redisMgr *pRedisMgr = NULL, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *bscSplit = ",")
    {
        m_db = db;
        m_name.assign(name);
	    return redis_map_map_base::init(pRedisMgr, run_cmd_Mod, syncChannel, syncdb, basic_sync_cmd, bscSplit);
	}
		 
	void init()
	{
	     return redis_map_map_base::init();
	}
	
	template<typename KeyType, typename FieldType, typename ValType>
    bool insert(KeyType &key, FieldType &field, ValType &val, bool isname = true)
    {
        string tmp_key;
        build_key(tmp_key, key, isname); 
        string tmp_field;
        bitTostring(field, tmp_field);      
        string tmp_val;
        bitTostring(val, tmp_val); 
        return redis_map_map_base::insert(tmp_key, tmp_field, tmp_val, m_db);     
    }
    
    template<typename KeyType, typename FieldType, typename ValType>
    bool find(KeyType &key, FieldType &field, ValType &val, bool isname = true)
    {
        string tmp_key;        
        build_key(tmp_key, key, isname);
        string tmp_field;
        bitTostring(field, tmp_field);                   
        string tmp_val;
        
        if (!redis_map_map_base::find(tmp_key, tmp_field, tmp_val, m_db) )
            return false;        
        if (tmp_val.empty())
            return false;
        stringTobit(val, tmp_val);    
        return true;
    }
    
    template<typename KeyType, typename FieldType>  
    bool erase(KeyType &key, FieldType &field, bool isname = true)
    {
        string tmp_key; 
        build_key(tmp_key, key, isname);
        string tmp_field;
        bitTostring(field, tmp_field);                  
        return redis_map_map_base::erase(tmp_key, tmp_field, m_db);
    }
    
    template<typename KeyType>
    int size(KeyType &key, bool isname = true)
    {
        string tmp_key;
        build_key(tmp_key, key, isname);     
        string tmp_size;
        if (redis_map_map_base::size(tmp_key, tmp_size, m_db))
        {
            return -1;
        }
        return atoi(tmp_size.c_str());            
    }
    
    template<typename KeyType>
    bool getallkey(vector<KeyType>& keys)
    {
        string filter(m_name);
        filter.append("*");
        redis_scan_keys(filter, keys, m_db);
    } 
    
    template<typename KeyType>
    inline void build_key(string &tmp_key, KeyType &key, bool isname = true)
    {
        if (isname)
            tmp_key = m_name + bitTostring(key); 
        else
            tmp_key = bitTostring(key); 
    }   
private:
    string m_name;
    int m_db;        
};

#endif /* REDIS_MAP_MAP_H_ */
