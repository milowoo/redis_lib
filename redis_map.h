/*
 *  redis_map.h
 *
 *  Created on: 2016Äê5ÔÂ25ÈÕ
 *      Author: root
 */

#ifndef REDIS_MAP_H_
#define REDIS_MAP_H_

#include "redis_map_map_base.h"
#include "misc.h"
	

class redis_map :public redis_map_map_base {
public:

	template<typename NameType>
    redis_map(int db, NameType name, redisMgr *pRedisMgr = NULL, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *bscSplit = ","):
	    redis_map_map_base(pRedisMgr, run_cmd_Mod, syncChannel, syncdb, basic_sync_cmd, bscSplit),
	    m_db(db) 
	{	   
        stringstream ss;
	    ss<<name;
	    m_name.assign(ss.str());   
	}
	
	redis_map():
	    m_db(0)
	{
	}
	
	~redis_map()
	{
	}
	
	template<typename NameType>
	bool init(int db, NameType &name, redisMgr *pRedisMgr, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *bscSplit = ",")
    {
        m_db = db;
        stringstream ss;
	    ss<<name;
	    m_name.assign(ss.str());  
	    return redis_map_map_base::init(pRedisMgr, run_cmd_Mod, syncChannel, syncdb, basic_sync_cmd, bscSplit);
	}
		 
	void init()
	{
	     return redis_map_map_base::init();
	}
	
	template<typename KeyType, typename ValType>
    bool insert(KeyType &key, ValType &val)
    {
        string tmp_field;
        bitTostring(key, tmp_field);      
        string tmp_val;
        bitTostring(val, tmp_val); 
        return redis_map_map_base::insert(m_name, tmp_field, tmp_val, m_db);     
    }    
    
    template<typename KeyType, typename ValType> 
    bool find(KeyType &key, ValType &val)
    {
        string tmp_field;
        bitTostring(key, tmp_field);                   
        string tmp_vals;
        
        if (!redis_map_map_base::find(m_name, tmp_field, tmp_vals, m_db) )
            return false;        
        if (tmp_vals.empty())
            return false;
        stringTobit(val, tmp_vals);    
        return true;
    }
    
    template<typename KeyType>  
    bool erase(KeyType &key)
    {
        string tmp_field;
        bitTostring(key, tmp_field);          
        return redis_map_map_base::erase(m_name, tmp_field, m_db);
    }
    
    int size()
    {
        string tmp_size;
        if (redis_map_map_base::size(m_name, tmp_size, m_db))
        {
            return -1;
        }
        return atoi(tmp_size.c_str());            
    }
    	

      
private:
    string m_name;
    int m_db;        
};

#endif /* REDIS_MAP_H_ */
