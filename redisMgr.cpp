/*
 * redisMgr.cpp
 *
 *  Created on: 2016Äê3ÔÂ30ÈÕ
 *      Author: root
 */

#include "redisMgr.h"
#include "redisOpt.h"
#include <limits.h>

redisMgr::redisMgr(const char* ip, uint16 port, const char* passwd, size_t num)
	: ip_(ip)
	, port_(port)
	, num_(num)
	, index_(0)
{
	for(size_t i=0; i<num; i++){
		redisOpt *predis = new redisOpt(ip, port, false, passwd);
		if (predis){
			predis->runloop();
			vec_redis.push_back(predis);
		}
	}

}

redisMgr::~redisMgr() {
	for (size_t i=0; i<num_; i++){
		delete vec_redis[i];
	}
}

redisOpt* redisMgr::getOne()
{
	if (0 == num_)
		return NULL;

	size_t i = index_ % num_;
	boost::mutex::scoped_lock lock(inc_mutex_);
	if (index_ > UINT_MAX - 1)
		index_ = 0;
	return vec_redis[i];

}

