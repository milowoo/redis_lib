#include "redis_map_map_base.h"
#include "redisOpt.h"


redis_map_map_base::redis_map_map_base(redisMgr *pRedisMgr)
    : basic_redis(pRedisMgr)
{
}

redis_map_map_base::redis_map_map_base()
{
}

redis_map_map_base::~redis_map_map_base()
{
} 
 
bool redis_map_map_base::insert(string &key, string &field, string &val, int db)
{   
    if (key.empty()||field.empty()||val.empty()) return false;
         
    if (key.size() + field.size() + val.size() + 48 > MAX_CMD_SIZE)
    {
        string query("HSET ");
        query.append(key);
        query.append(" ");
        query.append(field);  
        query.append(" ");
        query.append(val);  
        LOG_PRINT(log_info, "%s", query.c_str());
        return run_update_cmd(db, query.c_str());
    }
    else
    {
        char query[MAX_CMD_SIZE] = "";
	    snprintf(query, sizeof(query) - 1, "HSET %s %s %s", key.c_str(), field.c_str(), val.c_str());
	    LOG_PRINT(log_info, "%s", query);
	    return run_update_cmd(db, query);
    }
}

bool redis_map_map_base::find(string &key, string &field, string &val, int db)
{  
    if (key.empty()||field.empty()) return false;   
    if (key.size() + field.size() + val.size() + 48 > MAX_CMD_SIZE)
    {
        string query("HGET ");
        query.append(key);
        query.append(" ");
        query.append(field);
        LOG_PRINT(log_info, "%s", query.c_str());
        return run_cmd(db, val, query.c_str());
    }
    else
    {
        char query[MAX_CMD_SIZE] = "";
	    snprintf(query, sizeof(query) - 1, "HGET %s %s", key.c_str(), field.c_str());
	    LOG_PRINT(log_info, "%s", query);
	    return run_cmd(db, val, query);
    }
}

bool redis_map_map_base::erase(string &key, string &field, int db)
{    
    if (key.empty()||field.empty()) return false; 
    if (key.size() + field.size() + 48 > MAX_CMD_SIZE)
    {
        string query("HDEL ");
        query.append(key);
        query.append(" ");
        query.append(field);  
        LOG_PRINT(log_info, "%s", query.c_str());
        return run_update_cmd(db, query.c_str());
    }
    else
    {
        char query[MAX_CMD_SIZE] = "";
	    snprintf(query, sizeof(query) - 1, "HDEL %s %s", key.c_str(), field.c_str());
	    LOG_PRINT(log_info, "%s", query);
	    return run_update_cmd(db, query);
    }
}

bool redis_map_map_base::size(string &key, string &fieldnum, int db)
{
    if (key.empty()||fieldnum.empty()) return false; 
    if (key.size() + fieldnum.size() + 48 > MAX_CMD_SIZE)
    {
        string query("HLEN ");
        query.append(key);
        LOG_PRINT(log_info, "%s", query.c_str());
        return run_cmd(db, query.c_str());
    }
    else
    {
        char query[MAX_CMD_SIZE] = "";
	    snprintf(query, sizeof(query) - 1, "HLEN %s", key.c_str());
	    LOG_PRINT(log_info, "%s", query);
	    return run_cmd(db, fieldnum, query);
    }
}
