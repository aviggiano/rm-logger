#include "redismodule.h"
const char keys[] = "LOGGER.KEYS";
const char values[] = "LOGGER.VALUES";
const char timestamps[] = "LOGGER.TIMESTAMPS";
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

    RedisModule_ReplyWithCallReply(ctx, rep3);

    return REDISMODULE_OK;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {

    // Register the module itself – it’s called example and has an API version of 1
    if (RedisModule_Init(ctx, "LOGGER", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    // register our command – it is a write command, with one key at argv[1]
    if (RedisModule_CreateCommand(ctx, "LOGGER.PUB", LoggerPubCommand, "write", 1, 1, 1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}


