/*
 * redias_map_map_base.h
 *
 *  Created on: 2016Äê6ÔÂ01ÈÕ
 *      Author: root
 */

#ifndef REDIS_MAP_MAP_BASE_H_
#define REDIS_MAP_MAP_BASE_H_

#include "basic_redis.h"

class redis_map_map_base:public basic_redis {
public:
	redis_map_map_base(redisMgr *pRedisMgr);
	redis_map_map_base();
	~redis_map_map_base();  
    bool insert(string &key, string &field, string &val, int db = -1); 
    bool find(string &key, string &field, string &val, int db = -1);    
    bool erase(string &key, string &field, int db = -1);
    bool size(string &key, string &fieldnum, int db = -1);         
};

#endif /* REDIS_MAP_MAP_BASE_H_ */
