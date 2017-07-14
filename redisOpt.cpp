/*
 * redisOpt.cpp
 *
 *  Created on: 2016Äê2ÔÂ18ÈÕ
 *      Author: root
 */

#include "redisOpt.h"
#include "CLogThread.h"
#include "CAlarmNotify.h"

void connectCallback(const redisAsyncContext *c, int status) {
	redisOpt *pRedis = (redisOpt *)c->data;
    if (status != REDIS_OK) {
        LOG_PRINT(log_error, "redis error: %s [%s:%u]", c->errstr, pRedis->getip(), pRedis->getport());

        return;
    }
    pRedis->setconnstatus(true);
    LOG_PRINT(log_info, "redis Connected...[%s:%u]", pRedis->getip(), pRedis->getport());
}

void disconnectCallback(const redisAsyncContext *c, int status) {
	redisOpt *pRedis = (redisOpt *)c->data;
	pRedis->setconnstatus(false);

    if (status != REDIS_OK) {
    	LOG_PRINT(log_error, "redis error: %s [%s:%u]", c->errstr, pRedis->getip(), pRedis->getport());
        return;
    }

    LOG_PRINT(log_info, "redis Disconnected...[%s:%u]", pRedis->getip(), pRedis->getport());
}

void setCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = (redisReply *)r;
    redisOpt *pRedis = (redisOpt *)c->data;
    if (reply == NULL){
    	LOG_PRINT(log_error, "redis error: %s [%s:%u]", c->errstr, pRedis->getip(), pRedis->getport());
    	return;
    }
    //printf("argv[%s]: %s\n", (char*)privdata, reply->str);
}

static void timeout_cb(EV_P_ ev_timer *w, int revents)
{
	redisOpt *predis = (redisOpt*)w->data;
	if (predis)
		predis->redis_async_ping();
}

void onMessage(redisAsyncContext *c, void *reply, void *privdata) {
    redisReply *r = (redisReply*)reply;
    redisOpt *pRedis = (redisOpt*)privdata;
    if (reply == NULL)
    	return;
    if (r->type == REDIS_REPLY_ARRAY) {
        for (size_t j = 0; j < r->elements; j++) {
            printf("%u) %s\n", j, r->element[j]->str);
        }
        if (0 == strcasecmp(r->element[0]->str, "message")){
        	if (pRedis->redis_handle_msg)
        		pRedis->redis_handle_msg(r->element[1]->str, r->element[2]->str);
        }
    }
}

redisOpt::redisOpt(const char* ip, uint16_t port, bool bSubPub, const char *password)
	: bSubPub_(bSubPub)
	, ip_(ip)
	, port_(port)
	, c(NULL)
	, retrynum(0)
	, ac(NULL)
	, aysnc_retrynum(0)
	, reply(NULL)
	, connected_(false)
	, bReleased(false)
	, redis_handle_msg(NULL)
	, redis_conn_exception(NULL)
	, password_(NULL)

{
	if (password)
		password_ = strdup(password);
    m_hvalsName_field[KEY_HASH_ROOM_INFO] = "roomname busepwd strpwd nlevel nopstate nvcbid ngroupid nseats ncreatorid nopuserid0 nopuserid1 nopuserid2 nopuserid3 roomnotice0 roomnotice1 roomnotice2 roomnotice3";
    m_hvalsName_field[KEY_HASH_USER_INFO] = "vcbid userid nsvrid ngateid nk nb nd pGateObj pGateObjId nviplevel nyiyuanlevel nshoufulevel nzhongshenlevel ncaifulevel lastmonthcostlevel thismonthcostlevel thismonthcostgrade nisxiaoshou ngender ndevtype ncurpublicmicindex ninroomlevel usertype nheadid nstarflag nactivityflag nflowernum nyuanpiaonum bForbidChat calias ipaddr uuid gateip mediaip inroomstate";	
}

redisOpt::~redisOpt() {
	// TODO Auto-generated destructor stub
	if (c){
		redisFree(c);
		c = NULL;
	}
	bReleased = true;
	if (ac && !ac->err)
		redisAsyncDisconnect(ac);

	if (password_)
		free(password_);
}

void* redisOpt::thread_proc(void* arg)
{
	redisOpt* propt = (redisOpt*)arg;
	if (!propt)
		return NULL;

	signal(SIGPIPE, SIG_IGN);

	while (true){
		if (!propt->bReleased){
			propt->async_connect();
			sleep(10);
		}
	}
}

int redisOpt::runloop()
{
	redisThread.start(thread_proc, this);
	return 0;
}

int redisOpt::async_connect()
{
    ac = redisAsyncConnect(ip_.c_str(), port_);
    if (ac->err) {
        LOG_PRINT(log_error, "Error: %s\n", ac->errstr);
        return -1;
    }
    ac->data = this;

    struct ev_loop *epoller = ev_loop_new (EVBACKEND_EPOLL | EVFLAG_NOENV);
    redisLibevAttach(epoller, ac);
    redisAsyncSetConnectCallback(ac, connectCallback);
    redisAsyncSetDisconnectCallback(ac, disconnectCallback);
    if (password_){
    	redisAsyncCommand(ac, setCallback, NULL, "AUTH %s", password_);
    }
    if (bSubPub_)
    	redisAsyncCommand(ac, onMessage, this, "SUBSCRIBE %s", CHANNEL_REDIS_SYNC);
    else{
		ping_timer.data = this;
		ev_timer_init (&ping_timer, timeout_cb, 2, 30);
		ev_timer_start (epoller, &ping_timer);
    }

    ev_loop(epoller, 0);
    ev_loop_destroy(epoller);

    return 0;
}

int redisOpt::connect()
{
	struct timeval timeout = { 0, 500000 }; // 0.5 seconds

	int ntimes = 0;

START:
	ntimes++;
	if (ntimes > 3){
		LOG_PRINT(log_error, "Failed to connect after attempt 3 times");
		if (redis_conn_exception)
			redis_conn_exception();
		return -1;
	}

	if (c && c->err){
        redisFree(c);
        c = NULL;
	}

	if (NULL == c){
	    c = redisConnectWithTimeout(ip_.c_str(), port_, timeout);
	    if (c == NULL || c->err) {
	        if (c) {
	        	LOG_PRINT(log_error, "Connection error: %s", c->errstr);
	            redisFree(c);
	            c = NULL;
	        } else {
	        	LOG_PRINT(log_error, "Connection error: can't allocate redis context");
	        }
	        return -1;
	    }
	    if (password_){
	    	reply = (redisReply*)redisCommand(c, "AUTH %s", password_);
	    	if (NULL == reply){
	    		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
	    		redisFree(c);
	    		c = NULL;
	    		return -1;
	    	}
	    	freeReplyObject(reply);
	    }
	}
	else {
		reply = (redisReply*)redisCommand(c, "ping");
		if (NULL == reply){
			LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
			redisFree(c);
			c = NULL;
			goto START;
		}
		freeReplyObject(reply);
	}

	return 0;
}

int redisOpt::redis_set_test()
{
	if (!isconnected())
		return -1;

	return redisAsyncCommand(ac, setCallback, NULL, "set key 11");
}

int redisOpt::redis_async_ping()
{
	if (!isconnected())
		return -1;

	return redisAsyncCommand(ac, setCallback, NULL, "ping");
}

int redisOpt::redis_ping()
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c, "ping");
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}
	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_delkey(const char *key)
{
	if (!isconnected())
		return -1;

	return redisAsyncCommand(ac, setCallback, NULL, "del %s", key);
}

int redisOpt::redis_FlushAll(int db)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	char cmd[32] = {0};
	sprintf(cmd, "FLUSHDB");
	reply = (redisReply*)redisCommand(c, cmd);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_GetAllRoomids(vector<string> &roomids)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"SMEMBERS %s", KEY_SET_ROOMIDS);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}
	for (size_t i=0; i<reply->elements; i++){
		if (reply->element[i]->type ==  REDIS_REPLY_STRING)
			roomids.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_SetRoomid(vector<string> &roomids)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	string vals = vecToStr(roomids);
	char *str = (char*)malloc(vals.size() + 32);
	sprintf(str, "sadd %s %s", KEY_SET_ROOMIDS, vals.c_str());
	reply = (redisReply*)redisCommand(c, str);
	//reply = (redisReply*)redisCommand(c, "sadd %s %s", KEY_SET_ROOMIDS, vals.c_str());
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		if (str)
			free(str);
		return -1;
	}

	freeReplyObject(reply);
	if (str)
		free(str);

	return 0;
}

int redisOpt::redis_SetRoomid(const char* roomid)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c, "sadd %s %s", KEY_SET_ROOMIDS, roomid);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_SetRoomid(uint32 roomid)
{
	char sroomid[32] = {0};
	sprintf(sroomid, "%u", roomid);
	return redis_SetRoomid(sroomid);
}

int redisOpt::redis_DelRoomid(string &roomid)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c, "srem %s %s", KEY_SET_ROOMIDS, roomid.c_str());
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_SetRoomUserid(uint32 roomid, uint32 userid)
{
//	if (!isconnected())
//		return -1;
//
//	return redisAsyncCommand(ac, setCallback, NULL, "sadd %s:%u %u", KEY_HASH_ROOM_USERIDS, roomid, userid);

	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c, "sadd %s:%u %u", KEY_HASH_ROOM_USERIDS, roomid, userid);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_DelRoomUserid(uint32 roomid, uint32 userid)
{
//	if (!isconnected())
//		return -1;
//
//	return redisAsyncCommand(ac, setCallback, NULL, "srem %s:%u %u", KEY_HASH_ROOM_USERIDS, roomid, userid);

	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c, "srem %s:%u %u", KEY_HASH_ROOM_USERIDS, roomid, userid);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

typedef struct arg{
	bool breply;
	vector<string> *pdata;
}ARG_t;

void getAllUseridsCallback(redisAsyncContext *c, void *r, void *privdata) {
	ARG_t *parg = (ARG_t *)privdata;

	redisOpt *pRedis = (redisOpt *)c->data;

    redisReply *reply = (redisReply *)r;
    if (reply == NULL){
    	LOG_PRINT(log_error, "redis error: %s [%s:%u]", c->errstr, pRedis->getip(), pRedis->getport());

    	parg->breply = true;
    	return;
    }
    //printf("argv[%s]: %s\n", (char*)privdata, reply->str);

    parg->breply = true;
}

int redisOpt::redis_GetRoomAllUserids(uint32 roomid, vector<string> &userids)
{
	boost::mutex::scoped_lock lock(redis_mutex_);
	userids.clear();

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"SMEMBERS %s:%u", KEY_HASH_ROOM_USERIDS, roomid);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}
	for (size_t i=0; i<reply->elements; i++){
		if (reply->element[i]->type ==  REDIS_REPLY_STRING)
			userids.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_CountRoomUserid(uint32 roomid, uint32 userids)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"SISMEMBER %s:%u %u", KEY_HASH_ROOM_USERIDS, roomid, userids );
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return reply->integer;
}

int redisOpt::hmset( const string &key, const vector<string> &fields, const vector<string> &vals )
{
	if (fields.size() != vals.size()){
		LOG_PRINT(log_error, "the length not match for fields and vals");
		return -1;
	}

	vector<const char *> argv;
	vector<size_t> argvlen;

	static char cmd[] = "HMSET";
	argv.push_back( cmd );
	argvlen.push_back( sizeof(cmd)-1 );

	argv.push_back( key.c_str() );
	argvlen.push_back( key.size() );

	for (int i=0; i<fields.size(); i++){
		argv.push_back(fields[i].c_str());
		argvlen.push_back(fields[i].size());
		argv.push_back(vals[i].c_str());
		argvlen.push_back(vals[i].size());
	}


	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommandArgv(c, argv.size(), &(argv[0]), &(argvlen[0]) );
	if ( NULL == reply ){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject( reply );
	return 0;
}

int redisOpt::del( const string &key )
{
	vector<const char *> argv;
    vector<size_t> argvlen;

    static char cmd[] = "DEL";
	argv.push_back( cmd );
	argvlen.push_back( sizeof(cmd)-1 );

    argv.push_back( key.c_str() );
	argvlen.push_back( key.size() );

	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommandArgv(c, argv.size(), &(argv[0]), &(argvlen[0]) );
	if ( NULL == reply ){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject( reply );
	return 0;
}

int redisOpt::hincrby( const string &key, const vector<string> &fields, const vector<string> &vals )
{
	if (fields.size() != vals.size()){
		LOG_PRINT(log_error, "the length not match for fields and vals");
		return -1;
	}

	vector<const char *> argv;
	vector<size_t> argvlen;

	static char cmd[] = "HINCRBY";
	argv.push_back( cmd );
	argvlen.push_back( sizeof(cmd)-1 );

	argv.push_back( key.c_str() );
	argvlen.push_back( key.size() );

	for (int i=0; i<fields.size(); i++){
		argv.push_back(fields[i].c_str());
		argvlen.push_back(fields[i].size());
		argv.push_back(vals[i].c_str());
		argvlen.push_back(vals[i].size());
	}


	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommandArgv(c, argv.size(), &(argv[0]), &(argvlen[0]) );
	if ( NULL == reply ){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject( reply );
	return 0;
}

int redisOpt::redis_SetUserInfo(const char *userid, vector<string> &vals)
{
	boost::mutex::scoped_lock lock(redis_mutex_);


	std::stringstream ss;
	std::string key;
	vector<string> fields;

	ss << KEY_HASH_USER_INFO << ":" << userid;
	key = ss.str();

	fields.push_back("vcbid");
	fields.push_back("userid");
	fields.push_back("nsvrid");
	fields.push_back("ngateid");
	fields.push_back("nk");
	fields.push_back("nb");
	fields.push_back("nd");
	fields.push_back("pGateObj");
	fields.push_back("pGateObjId");
	fields.push_back("nviplevel");
	fields.push_back("nyiyuanlevel");
	fields.push_back("nshoufulevel");
	fields.push_back("nzhongshenlevel");
	fields.push_back("ncaifulevel");
	fields.push_back("lastmonthcostlevel");
	fields.push_back("thismonthcostlevel");
	fields.push_back("thismonthcostgrade");
	fields.push_back("nisxiaoshou");
	fields.push_back("ngender");
	fields.push_back("ndevtype");
	fields.push_back("ncurpublicmicindex");
	fields.push_back("ninroomlevel");
	fields.push_back("usertype");
	fields.push_back("nheadid");
	fields.push_back("nstarflag");
	fields.push_back("nactivityflag");
	fields.push_back("nflowernum");
	fields.push_back("nyuanpiaonum");
	fields.push_back("bForbidChat");
	fields.push_back("calias");
	fields.push_back("ipaddr");
	fields.push_back("uuid");
	fields.push_back("gateip");
	fields.push_back("mediaip");
	fields.push_back("inroomstate");
	fields.push_back("nuserviplevel");


	return hmset(key, fields, vals);
}

int redisOpt::redis_SetUserInfo(string &userid, vector<string> &vals)
{
	return redis_SetUserInfo(userid.c_str(), vals);
}

int redisOpt::redis_GetUserInfo(uint32 nuserid, vector<string> &result)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	result.clear();
	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommand(c, "hvals %s:%u", KEY_HASH_USER_INFO, nuserid);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}
	for (size_t i=0; i<reply->elements; i++){
		if (reply->element[i]->type ==  REDIS_REPLY_STRING)
			result.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	return 0;
}

// remove user info from redis server
int redisOpt::redis_DelUserInfo(uint32 nuserid)
{
//	if (!isconnected())
//		return -1;
//
//	return redisAsyncCommand(ac, setCallback, NULL, "del %s:%u", KEY_HASH_USER_INFO, nuserid);

	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"del %s:%u", KEY_HASH_USER_INFO, nuserid);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_SetMicState(uint32 nuserid, int micindex)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	char sztemp[32];
	vector<string> fields;
	vector<string> vals;
	string key;
	fields.push_back("ncurpublicmicindex");
	sprintf(sztemp, "%d", micindex);
	vals.push_back(sztemp);
	sprintf(sztemp, "%s:%u", KEY_HASH_USER_INFO, nuserid);
	key = sztemp;

	return hmset(key, fields, vals);
}

int redisOpt::redis_writeMsg(string &msg)
{
	LOG_PRINT(log_info, "write msg to redis: %s", msg.c_str());
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommand(c,"lpush %s %s", KEY_ROOM_MSG, msg.c_str());
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}
	freeReplyObject(reply);
	return 0;
}

int redisOpt::redis_readMsg(vector<string> &result)
{
	boost::mutex::scoped_lock lock(redis_mutex_);
	result.clear();
	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommand(c,"brpop %s 0", KEY_ROOM_MSG);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}
	for(size_t i=0; i<reply->elements; i++){
		result.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_publish(const char *msg)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommand(c,"publish %s %s", CHANNEL_REDIS_SYNC, msg);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}


int redisOpt::redis_GetAllServerList(vector<string> &servers)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"SMEMBERS %s", KEY_SERVER_SET);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}
	for (size_t i=0; i<reply->elements; i++){
		if (reply->element[i]->type ==  REDIS_REPLY_STRING)
			servers.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_RegisterServer(std::string key)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

    char str[128] = "";
	snprintf(str, 128, "sadd %s %s", KEY_SERVER_SET, key.c_str());
	reply = (redisReply*)redisCommand(c, str);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_GetServerConnNum(std::string key, int &val)
{
    boost::mutex::scoped_lock lock(redis_mutex_);

    if (connect() < 0)
        return -1;

    reply = (redisReply*)redisCommand(c,"lSize %s", key.c_str());
    if (NULL == reply){
    	LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
        redisFree(c);
        c = NULL;
        return -1;
    }
    for (size_t i=0; i<reply->elements; i++){
        if (reply->element[i]->type ==  REDIS_REPLY_STRING)
        {
            val = atoi(reply->element[i]->str);
            return 0;
        }
    }
    freeReplyObject(reply);

    return 0;
}

int redisOpt::redis_UpdateServerInfo( const string &key, int status )
{
	boost::mutex::scoped_lock lock(redis_mutex_);

    if (connect() < 0)
        return -1;

    reply = (redisReply*)redisCommand(c,"HMSET %s status %d", key.c_str(), status);
    if (NULL == reply){
    	LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
        redisFree(c);
        c = NULL;
        return -1;
    }
    freeReplyObject(reply);

    return 0;
}

int redisOpt::redis_DelServerConnList(const string &key)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"del conn:%s", key.c_str());
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_DelServerConn(const string &key, const string &value)
{
	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"lrem conn:%s 0 %s", key.c_str(), value.c_str());
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_hvals_set(vector<string> &key_vals, vector<string> &fields)
{
	boost::mutex::scoped_lock lock(redis_mutex_);
    if (key_vals.empty()) return -1;       
    string key;
    key.swap(key_vals[0]);
    key_vals.erase(key_vals.begin());
	return hmset(key, fields, key_vals);
}
      
int redisOpt::redis_hvals_set(string key, string &hvalsName, vector<string> &vals, vector<string> &fields)
{
	boost::mutex::scoped_lock lock(redis_mutex_);
    key = hvalsName + ":" + key;
	return hmset(key, fields, vals);
}

int redisOpt::redis_hvals_set(string key, string &fields, vector<string> &vals)
{
	boost::mutex::scoped_lock lock(redis_mutex_);
	vector<string> vecfields = strToVec(fields, ',');

	return hmset(key, vecfields, vals);
}

int redisOpt::redis_hvals_find(string key, string &hvalsName, vector<string> &vals, string &field)
{
    key = hvalsName + ":" + key;
    return redis_hvals_find(key, vals, field);
}

int redisOpt::redis_hvals_find(string key, vector<string> &vals, string &field)
{
	boost::mutex::scoped_lock lock(redis_mutex_);
	vals.clear();
	if (connect() < 0)
		return -1;

	char *str = (char*)malloc(field.size() + 32);
	sprintf(str, "hmget %s %s", key.c_str(), field.c_str());

	reply = (redisReply*)redisCommand(c, str);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		if (str)
			free(str);
		return -1;
	}
	for (size_t i=0; i<reply->elements; i++){
		if (reply->element[i]->type ==  REDIS_REPLY_STRING)
			vals.push_back(reply->element[i]->str);
	}
	freeReplyObject(reply);

	if (str)
		free(str);
	return 0;
}


int redisOpt::redis_hvals_delete(string key, string &hvalsName)
{

	boost::mutex::scoped_lock lock(redis_mutex_);

	if (connect() < 0)
		return -1;

	reply = (redisReply*)redisCommand(c,"del %s:%s", hvalsName.c_str(), key.c_str());
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject(reply);

	return 0;
}

int redisOpt::redis_hvals_init(string &hvalsName, vector<string> &fields, HVALSNAME_MAP &hvalsName_map)
{
    pair<HVALSNAME_MAP::iterator,bool> ret;
    ret = hvalsName_map.insert(make_pair(hvalsName, vector<string>(fields)));
    if (!ret.second)
        return -1;
	return 0;
}

int redisOpt::redis_hvals_keys(string &hvalsName, vector<string> &vals)
{
	boost::mutex::scoped_lock lock(redis_mutex_);
	if (connect() < 0)
		return -1;
	int cursor = 0;
	do {	      
    	reply = (redisReply*)redisCommand(c,"SCAN %d match %s:*", cursor, hvalsName.c_str());
    	if (NULL == reply){
    		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
    		redisFree(c);
    		c = NULL;
    		return -1;
    	}
    	
    	if (reply->type ==  REDIS_REPLY_ARRAY && reply->elements > 1) {    	    
    	    cursor = atoi(reply->element[0]->str);
    	} else {
    	    freeReplyObject(reply);
            return -1;
    	}   
    	    
    	for (size_t i=0; i<reply->element[1]->elements; i++){
    		if (reply->element[1]->element[i]->type ==  REDIS_REPLY_STRING)
    			vals.push_back(reply->element[1]->element[i]->str);
    	}
    } while (cursor);
    
	freeReplyObject(reply);
}


int redisOpt::redis_run_cmdArgv(int argvnum, const char **argv, size_t *argvlen)
{
	if (argvnum < 0 || argv == NULL || argvlen == NULL){
		LOG_PRINT(log_error, "wrong param");
		return -1;
	}
	
	if (connect() < 0)
		return -1;
	reply = (redisReply*)redisCommandArgv(c, argvnum, argv, argvlen);
	if ( NULL == reply ){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}

	freeReplyObject( reply );
	return 0;
}

int redisOpt::redis_run_cmd(const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);	
    int ret = redis_run_cmdv(fmt, ap); 
    va_end(ap);  
    return ret;
}
      
int redisOpt::redis_run_cmdv(const char* fmt, va_list ap)
{
    boost::mutex::scoped_lock lock(redis_mutex_);

    redisReply* tmp_res = NULL;
    bool ret = redis_run_cmdv(&tmp_res, fmt, ap);
	freeReplyObject(tmp_res);
	return ret; 

	return 0;
}

int redisOpt::redis_run_cmd(redisReply** res, const char* fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);	
    int ret = redis_run_cmdv(res, fmt, ap); 
    va_end(ap);  
    return ret;
}

int redisOpt::redis_run_cmdv(redisReply** res, const char* fmt, va_list ap)
{
    
	if (connect() < 0) return -1;   
	*res = (redisReply*)redisvCommand(c, fmt, ap);
	if (NULL == reply){
		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
		redisFree(c);
		c = NULL;
		return -1;
	}	
	LOG_PRINT(log_debug, "success!");
	return 0;
}

boost::mutex& redisOpt::get_redis_mutex()
{
    return redis_mutex_;
}

int redisOpt::redis_scan_keys(string &filter, vector<string> &vals)
{
	if (connect() < 0)
		return -1;
	int cursor = 0;
	do {	      
    	reply = (redisReply*)redisCommand(c,"SCAN %d match %s", cursor, filter.c_str());
    	if (NULL == reply){
    		LOG_PRINT(log_error, "err:%d, err_str:%s", c->err, c->errstr);
    		redisFree(c);
    		c = NULL;
    		return -1;
    	}
    	
    	if (reply->type ==  REDIS_REPLY_ARRAY && reply->elements > 1) {    	    
    	    cursor = atoi(reply->element[0]->str);
    	} else {
    	    freeReplyObject(reply);
            return -1;
    	}   
    	    
    	for (size_t i=0; i<reply->element[1]->elements; i++){
    		if (reply->element[1]->element[i]->type ==  REDIS_REPLY_STRING)
    			vals.push_back(reply->element[1]->element[i]->str);
    	}
    } while (cursor);
    
	freeReplyObject(reply);
}
