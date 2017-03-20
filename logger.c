#include "redismodule.h"
#include <limits.h>
const char keys[] = "LOGGER.KEYS";
const char values[] = "LOGGER.VALUES";
const char timestamps[] = "LOGGER.TIMESTAMPS";
const char clients[] = "LOGGER.CLIENTS";
const char unknown[] = "LOGGER.UNKNOWN";
/**
 * LOGGER.PUB <key> <value>
 * */
int LoggerPubCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }
    RedisModule_AutoMemory(ctx);

    RedisModuleString *rkeys = RedisModule_CreateString(ctx, keys, sizeof(keys)-1);
    RedisModuleString *rvalues = RedisModule_CreateString(ctx, values, sizeof(values)-1);
    RedisModuleString *rtimestamps = RedisModule_CreateString(ctx, timestamps, sizeof(timestamps)-1);

    RedisModuleCallReply *rep1 = RedisModule_Call(ctx, "LPUSH", "ss", rkeys, argv[1]);
    RedisModuleCallReply *rep2 = RedisModule_Call(ctx, "LPUSH", "ss", rvalues, argv[2]);
    RedisModuleCallReply *rep3 = RedisModule_Call(ctx, "LPUSH", "sl", rtimestamps, RedisModule_Milliseconds());

    RedisModuleCallReply *rep4 = RedisModule_Call(ctx, "PUBLISH", "ss", argv[1], argv[2]);

    RedisModule_ReplyWithCallReply(ctx, rep4);

    return REDISMODULE_OK;
}
/**
 * LOGGER.HISTORY [<timestamp_from> [<timestamp_to>]]
 * */
int LoggerHistoryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc > 3) {
        return RedisModule_WrongArity(ctx);
    }
    long long timestamp_from = LLONG_MIN;
    long long timestamp_to = LLONG_MAX;
    if(argc == 2) {
        RedisModule_StringToLongLong(argv[1],&timestamp_from);
    }
    if(argc == 3) {
        RedisModule_StringToLongLong(argv[2],&timestamp_to);
    }

    RedisModule_AutoMemory(ctx);

    RedisModuleString *rkeys = RedisModule_CreateString(ctx, keys, sizeof(keys)-1);
    RedisModuleString *rvalues = RedisModule_CreateString(ctx, values, sizeof(values)-1);
    RedisModuleString *rtimestamps = RedisModule_CreateString(ctx, timestamps, sizeof(timestamps)-1);

    RedisModuleCallReply *rep_keys = RedisModule_Call(ctx,"LRANGE","sll", rkeys,(long long)0,(long long)-1);
    RedisModuleCallReply *rep_values = RedisModule_Call(ctx,"LRANGE","sll", rvalues,(long long)0,(long long)-1);
    RedisModuleCallReply *rep_timestamps = RedisModule_Call(ctx,"LRANGE","sll", rtimestamps,(long long)0,(long long)-1);
    size_t total = RedisModule_CallReplyLength(rep_keys);
    size_t items = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    for (size_t i = 0; i < total; i++) {
        RedisModuleCallReply *rtimestamp = RedisModule_CallReplyArrayElement(rep_timestamps,i);
        RedisModuleString *timestamp_str = RedisModule_CreateStringFromCallReply(rtimestamp);
        long long timestamp;
        RedisModule_StringToLongLong(timestamp_str, &timestamp);
        if(timestamp >= timestamp_from && timestamp <= timestamp_to) {
            RedisModuleCallReply *rkey = RedisModule_CallReplyArrayElement(rep_keys,i);
            RedisModuleCallReply *rvalue = RedisModule_CallReplyArrayElement(rep_values,i);
            RedisModule_ReplyWithCallReply(ctx,rkey);
            RedisModule_ReplyWithCallReply(ctx,rvalue);
            RedisModule_ReplyWithCallReply(ctx,rtimestamp);
            items += 3;
        }
    }
    RedisModule_ReplySetArrayLength(ctx, items);


    return REDISMODULE_OK;
}
void LoggerConnectionCallback(RedisModuleCtx * ctx) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    unsigned long long client_id = RedisModule_GetClientId(ctx);
    RedisModule_Log(ctx, "notice", "Client id %llu connected", client_id);

    RedisModuleString *rclient_id = RedisModule_CreateStringFromLongLong(ctx, (long long) client_id);
    RedisModuleString *runknown = RedisModule_CreateString(ctx, unknown, sizeof(unknown)-1);
    RedisModuleString *rclients = RedisModule_CreateString(ctx, clients, sizeof(clients)-1);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, rclients, REDISMODULE_READ|REDISMODULE_WRITE);
    RedisModule_HashSet(key, REDISMODULE_HASH_NONE, rclient_id, runknown, NULL);
    RedisModule_CloseKey(key);
}
void LoggerDisconnectionCallback(RedisModuleCtx * ctx) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    unsigned long long client_id = RedisModule_GetClientId(ctx);
    RedisModule_Log(ctx, "notice", "Client id %llu disconnected", client_id);

    RedisModuleString *rclient_id = RedisModule_CreateStringFromLongLong(ctx, (long long) client_id);
    RedisModuleString *rclients = RedisModule_CreateString(ctx, clients, sizeof(clients)-1);

    RedisModule_SelectDb(ctx, 0);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, rclients, REDISMODULE_READ|REDISMODULE_WRITE);
    RedisModule_HashSet(key, REDISMODULE_HASH_NONE, rclient_id, REDISMODULE_HASH_DELETE, NULL);
    RedisModule_CloseKey(key);
}
int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    // Register the module itself
    if (RedisModule_Init(ctx, "LOGGER", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_HookToConnection(ctx, LoggerConnectionCallback) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_HookToDisconnection(ctx, LoggerDisconnectionCallback) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // register our command
    if (RedisModule_CreateCommand(ctx, "LOGGER.PUB", LoggerPubCommand, "write pubsub", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "LOGGER.HISTORY", LoggerHistoryCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}


