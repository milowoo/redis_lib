
#ifndef BASIC_REDIS_H_
#define BASIC_REDIS_H_

#include "redisMgr.h"
#include "redisOpt.h"
#include "cmd_vchat.h"
#include "redis_def.h"

enum {
	NOSYNC  = 0,	//不同步
	SYNC    = 1,	//同步
};

class basic_redis {
public:
	basic_redis(redisMgr *pRedisMgr, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *m_bscSplit = ",");
	basic_redis();
	~basic_redis();
	bool init(redisMgr *pRedisMgr, int run_cmd_Mod = NOSYNC, const char *syncChannel = KEY_ROOM_MSG, int syncdb = 0, int basic_sync_cmd = BASIC_SYNC, const char *m_bscSplit = ","); 
	void init();
	bool setSyncMod(int run_cmd_Mod);
    static bool setdb(int db, redisOpt *pRedisData, bool Islock = true);
    bool cleandb(int db);
    bool cleandb();
    bool run_update_cmd(int db, const char* fmt, ...);
    
    bool sync_run_cmd(int db, const char* query, ...);
    bool sync_run_cmd(const char *syncchannel, int syncdb, int db, const char* query);
    
    bool run_cmd(int db, const char* fmt, ...);
    bool run_cmdv(int db, const char* fmt, va_list ap);
    bool run_cmd(int db, string &res, const char* fmt, ...);   
    bool run_cmd(int db, vector<string> &res, const char* fmt, ...); 
    bool run_cmdv(int db, vector<string> &res, const char* fmt, va_list ap);   
    bool run_cmd(int db, redisReply** res, const char* fmt, ...); 
    bool run_cmdv(int db, redisReply** res, const char* fmt, va_list ap);        
    bool redis_scan_keys(string &filter, vector<string> &val, int db = -1);
    
protected:
    redisMgr *m_pRedisMgr;
    
    int m_basic_sync_cmd; 
    string m_bscSplit;
    string m_syncChannel;
    int m_syncdb;
    int m_run_cmd_Mod;
    vector<bool(basic_redis::*)(int, const char*, ...)> m_run_cmd;  
};

#endif /* BASIC_REDIS_H_ */
