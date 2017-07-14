/*
 * redisOpt.h
 *
 *  Created on: 2016Äê2ÔÂ18ÈÕ
 *      Author: root
 */

#ifndef REDISOPT_H_
#define REDISOPT_H_

#include "inc.h"
#include "misc.h"
// hiredis
#include "../../lib/hiredis/hiredis.h"
#include "../../lib/hiredis/async.h"
#include "../../lib/hiredis/adapters/libev.h"

#include "redis_def.h"


typedef void (*fn_REDIS_HANDLE_MSG)(const char* channel, const char* msg);
typedef void (*fn_REDIS_CONN_EXCEPTION)();
typedef map<string, vector<string> > HVALSNAME_MAP;
#define MAX_CMD_SIZE 4096
class redisOpt {
public:
	redisOpt(const char* ip, uint16_t port, bool bSubPub = false, const char* password = NULL);
	virtual ~redisOpt();

	static void* thread_proc(void* arg);
	int runloop();
	int async_connect();
	int connect();
	const char* getip() const { return ip_.c_str(); }
	uint16 getport() const { return port_; }
	void setconnstatus(bool connected) {
		boost::mutex::scoped_lock lock(redis_async_mutex_);
		connected_ = connected;
	}
	bool isconnected() {
		boost::mutex::scoped_lock lock(redis_async_mutex_);
		return connected_;
	}

	void redis_SetHandleMsgProc(fn_REDIS_HANDLE_MSG pfn) { redis_handle_msg = pfn; }
	void redis_ConnExceptCallback(fn_REDIS_CONN_EXCEPTION pfn) { redis_conn_exception = pfn; }
	int redis_async_ping();
	int redis_ping();
	int redis_set_test();
	int redis_delkey(const char *key);

	int redis_FlushAll(int db = 0);
	int redis_GetAllRoomids(vector<string> &roomids);
	int redis_SetRoomid(vector<string> &roomids);
	int redis_SetRoomid(const char* roomid);
	int redis_SetRoomid(uint32 roomid);
	int redis_DelRoomid(string &roomid);
	int redis_SetRoomUserid(uint32 roomid, uint32 userid);
	int redis_DelRoomUserid(uint32 roomid, uint32 userid);
	int redis_GetRoomAllUserids(uint32 roomid, vector<string> &userids);
	int redis_CountRoomUserid(uint32 roomid, uint32 userids);
	int redis_UserInRoom(uint32 roomid, uint32 userid);
	int redis_SetUserInfo(const char *userid, vector<string> &vals);
	int redis_SetUserInfo(string &userid, vector<string> &vals);
	int redis_GetUserInfo(uint32 nuserid, vector<string> &result);
	int redis_DelUserInfo(uint32 nuserid);
	int redis_SetMicState(uint32 nuserid, int micindex);
	int redis_writeMsg(string &msg);
	int redis_readMsg(vector<string> &result);
	int redis_publish(const char *msg);
    int hincrby( const string &key, const vector<string> &fields, const vector<string> &vals );
    int del( const string &key );
    int redis_IsMember(const char *key, const char *val);
    
    int redis_hvals_init(string &hvalsName, vector<string> &fields, HVALSNAME_MAP &hvalsName_map);
    int redis_hvals_keys(string &hvalsName, vector<string> &vals); 
    int redis_hvals_set(string key, string &hvalsName, vector<string> &vals, vector<string> &fields);
    int redis_hvals_set(string key, string &fields, vector<string> &vals);
    int redis_hvals_set(vector<string> &key_vals, vector<string> &fields);
    int redis_hvals_find    (string key, string &hvalsName, vector<string> &vals, string &field);
    int redis_hvals_find    (string key, vector<string> &vals, string &field);
    int redis_hvals_delete(string key, string &hvalsName);
    
    int redis_run_cmd(const char* fmt, ...);
    int redis_run_cmdv(const char* fmt, va_list ap);
    
    int redis_run_cmd(redisReply** res, const char* fmt, ...);
    int redis_run_cmdv(redisReply** res, const char* fmt, va_list ap);    
    int redis_run_cmdArgv(int argvnum, const char **argv, size_t *argvlen);
    
    boost::mutex& get_redis_mutex();
    int redis_scan_keys(string &filter, vector<string> &vals);
    //nodemgr
    int redis_GetAllServerList(vector<string> &servers);
    int redis_RegisterServer(std::string key);
    int redis_GetServerConnNum(std::string key, int &val);
    int redis_UpdateServerInfo( const string &key, int status );
    int redis_DelServerConn(const string &key, const string &value);
    int redis_DelServerConnList(const string &key);
public:
	int hmset(const string &key, const vector<string> &fields, const vector<string> &vals );
public:
	fn_REDIS_HANDLE_MSG redis_handle_msg;
	fn_REDIS_CONN_EXCEPTION redis_conn_exception;
	map<string, string> m_hvalsName_field;
protected:
	SL_Thread<SL_Sync_ThreadMutex> redisThread;
private:
	bool bSubPub_;
	ev_timer ping_timer;
	boost::mutex redis_mutex_;
	boost::mutex redis_async_mutex_;
	bool connected_;
    redisContext *c;
    uint16 retrynum;
    redisAsyncContext *ac;
    uint16 aysnc_retrynum;
    redisReply *reply;
	string ip_;
	uint16_t port_;
	bool bReleased;
	char *password_;
};

typedef boost::shared_ptr<redisOpt> redisOpt_ptr;

#endif /* REDISOPT_H_ */
