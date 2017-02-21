#include "redismodule.h"
const char keys[] = "LOGGER.KEYS";
const char values[] = "LOGGER.VALUES";
const char timestamps[] = "LOGGER.TIMESTAMPS";
const char heartbeat[] = "LOGGER.HEARTBEAT";
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
 * LOGGER.HISTORY 
 * */
int LoggerHistoryCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_AutoMemory(ctx);

    RedisModuleString *rkeys = RedisModule_CreateString(ctx, keys, sizeof(keys)-1);
    RedisModuleString *rvalues = RedisModule_CreateString(ctx, values, sizeof(values)-1);
    RedisModuleString *rtimestamps = RedisModule_CreateString(ctx, timestamps, sizeof(timestamps)-1);

    RedisModuleCallReply *rep_keys = RedisModule_Call(ctx,"LRANGE","sll", rkeys,(long long)0,(long long)-1);
    RedisModuleCallReply *rep_values = RedisModule_Call(ctx,"LRANGE","sll", rvalues,(long long)0,(long long)-1);
    RedisModuleCallReply *rep_timestamps = RedisModule_Call(ctx,"LRANGE","sll", rtimestamps,(long long)0,(long long)-1);
    size_t total = RedisModule_CallReplyLength(rep_keys);
    RedisModule_ReplyWithArray(ctx, 3 * total);
    for (size_t i = 0; i < total; i++) {
        RedisModuleCallReply *key = RedisModule_CallReplyArrayElement(rep_keys,i);
        RedisModuleCallReply *value = RedisModule_CallReplyArrayElement(rep_values,i);
        RedisModuleCallReply *timestamp = RedisModule_CallReplyArrayElement(rep_timestamps,i);
        RedisModule_ReplyWithCallReply(ctx,key);
        RedisModule_ReplyWithCallReply(ctx,value);
        RedisModule_ReplyWithCallReply(ctx,timestamp);
    }

    return REDISMODULE_OK;
}
int RedisModule_OnLoad(RedisModuleCtx *ctx) {

    // Register the module itself – it’s called example and has an API version of 1
    if (RedisModule_Init(ctx, "LOGGER", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // register our command – it is a write command, with one key at argv[1]
    if (RedisModule_CreateCommand(ctx, "LOGGER.PUB", LoggerPubCommand, "write pubsub", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }
    if (RedisModule_CreateCommand(ctx, "LOGGER.HISTORY", LoggerHistoryCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}


