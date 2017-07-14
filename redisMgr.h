/*
 * redisMgr.h
 *
 *  Created on: 2016Äê3ÔÂ30ÈÕ
 *      Author: root
 */

#ifndef REDISMGR_H_
#define REDISMGR_H_
#include "inc.h"

class redisOpt;

class redisMgr {
public:
	redisMgr(const char* ip, uint16 port, const char* passwd, size_t num);
	virtual ~redisMgr();

	redisOpt* getOne();
private:
	vector<redisOpt*> vec_redis;
	string ip_;
	uint16 port_;
	size_t num_;
	uint32 index_;
	boost::mutex inc_mutex_;
};

#endif /* REDISMGR_H_ */
