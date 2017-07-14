/*
 * redias_map_set_base.h
 *
 *  Created on: 2016Äê5ÔÂ25ÈÕ
 *      Author: root
 */

#ifndef _REDIS_MAP_SET_BASE_H_
#define _REDIS_MAP_SET_BASE_H_

#include "basic_redis.h"

class redis_map_set_base:public basic_redis {
public:
	redis_map_set_base(redisMgr *pRedisMgr);
	redis_map_set_base();
	~redis_map_set_base();  
    bool insert(string &key, string &val, int db = -1); 
    bool find(string &key, vector<string> &val, int db = -1);    
    bool erase(string &key, string &val, int db = -1);  
    int count(string &key, string &val, int db = -1);       
};

#endif /* _REDIS_MAP_SET_BASE_H_ */
