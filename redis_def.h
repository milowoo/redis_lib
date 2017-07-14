/*
 * redis_def.h
 *
 *  Created on: 2016年2月2日
 *      Author: root
 */

#ifndef REDIS_DEF_H_
#define REDIS_DEF_H_

#define KEY_SET_GATE_SVR "gate"
#define KEY_SET_ROOMIDS	"roomids"
#define KEY_HASH_ROOM_INFO "room"
#define KEY_HASH_ROOM_USERIDS "room_userids"
#define KEY_HASH_ROOM_MGR "roommgr_info"
#define KEY_HASH_USER_INFO "user"
#define KEY_HASH_USER_ROOM_ID "user_room"
#define KEY_ROOM_MSG "room_msg_tt"
#define KEY_HASH_TEAMTOPMONEY "team_top_money"
#define KEY_SERVER_SET "99cjserverset"
#define KEY_VISTOR_INFO "vistor"

#define CHANNEL_ROOM_MSG  "room_msg"  //消息结构 (svrid roommsgid userid roomid [...])
#define CHANNEL_GATE_DISCONN_MSG  "room_gate_msg"  //消息结构(svrid gateid)
#define CHANNEL_REDIS_SYNC  "redis_sync"
#define REDIS_DELIMITER  "{卍卐△▽}"
#define BLANK  "#"

enum {
    DB_ROOM     = 0,
    DB_USER_MGR = 1,
    DB_RECORD   = 2,
	DB_VIPMGR   = 3,
};

#endif /* REDIS_DEF_H_ */
