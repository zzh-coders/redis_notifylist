#define REDISMODULE_EXPERIMENTAL_API

#include "../redismodule.h"
#include <stdlib.h>
#include <ctype.h>
#include <string.h>

static RedisModuleDict *Keyspace;
static int is_sub;

static char *str_replace(const char *string, const char *substr, const char *replacement) {
    char *tok = NULL;
    char *newstr = NULL;
    char *oldstr = NULL;
    int oldstr_len = 0;
    int substr_len = 0;
    int replacement_len = 0;

    newstr = strdup(string);
    substr_len = strlen(substr);
    replacement_len = strlen(replacement);

    if (substr == NULL || replacement == NULL) {
        return newstr;
    }

    while ((tok = strstr(newstr, substr))) {
        oldstr = newstr;
        oldstr_len = strlen(oldstr);
        newstr = (char *) malloc(sizeof(char) * (oldstr_len - substr_len + replacement_len + 1));

        if (newstr == NULL) {
            free(oldstr);
            return NULL;
        }

        memcpy(newstr, oldstr, tok - oldstr);
        memcpy(newstr + (tok - oldstr), replacement, replacement_len);
        memcpy(newstr + (tok - oldstr) + replacement_len, tok + substr_len, oldstr_len - substr_len - (tok - oldstr));
        memset(newstr + oldstr_len - substr_len + replacement_len, 0, 1);

        free(oldstr);
    }

    return newstr;
}

static int is_begin_with(const char *str1, const char *str2) {
    if (str1 == NULL || str2 == NULL)
        return -1;
    int len1 = strlen(str1);
    int len2 = strlen(str2);
    if ((len1 < len2) || (len1 == 0 || len2 == 0))
        return -1;
    const char *p = str2;
    int i = 0;
    while (*p != '\0') {
        if (*p != str1[i])
            return 0;
        p++;
        i++;
    }
    return 1;
}

void PushToList(RedisModuleCtx *ctx, const char *event, const char *key_str, RedisModuleString *list_name) {
    if (strcmp(event, "set") == 0 || strcmp(event, "expired") == 0 || strcmp(event, "expire") == 0) {
        RedisModuleString *s = RedisModule_CreateStringPrintf(ctx,
                                                              "{\"key\":\"%s\",\"op\":\"%s\"}",
                                                              key_str,
                                                              event);
        RedisModule_Call(ctx, "RPUSH", "ss", list_name, s);
        RedisModule_FreeString(ctx, s);
    }
}

static int Pubsub_Notification(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key) {
    REDISMODULE_NOT_USED(type);

    if (RedisModule_DictSize(Keyspace) == 0) {
        return REDISMODULE_ERR;
    }
    size_t str_len;
    const char *key_str = RedisModule_StringPtrLen(key, &str_len);
    RedisModuleString *val = RedisModule_DictGet(Keyspace, key, NULL);
    if (val != NULL) {
        //进行精确查找
        PushToList(ctx, event, key_str, val);
    }

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(Keyspace, ">=", NULL, 0);
    //这是模糊查找
    char *dict_key;
    size_t key_len;
    while ((dict_key = RedisModule_DictNextC(iter, &key_len, NULL)) != NULL) {
        const char *dict_key_str = dict_key;
        dict_key = NULL;
        if (strstr(dict_key_str, "*") < 0) {
            continue;
        }

        const char *key_k = str_replace(dict_key_str, "*", "");
        if (is_begin_with(key_str, key_k) == 1) {
            RedisModuleString *r_dict_key = RedisModule_CreateString(ctx, dict_key_str, strlen(dict_key_str));
            RedisModuleString *s = RedisModule_DictGet(Keyspace, r_dict_key, NULL);
            if (s != NULL) {
                PushToList(ctx, event, key_str, s);
            }
            RedisModule_FreeString(ctx, r_dict_key);
        }
    }

    return REDISMODULE_OK;
}

int NotifyListCommand_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3)
        return RedisModule_WrongArity(ctx);
    if (RedisModule_DictGet(Keyspace, argv[1], NULL) == NULL) {
        RedisModule_DictSet(Keyspace, argv[1], argv[2]);
    } else {
        RedisModule_DictReplace(Keyspace, argv[1], argv[2]);
    }

    /* We need to keep a reference to the value stored at the key, otherwise
     * it would be freed when this callback returns. */
    RedisModule_RetainString(NULL, argv[2]);
    if (is_sub == 0 &&
        RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_EXPIRED | REDISMODULE_NOTIFY_STRING,
                                              Pubsub_Notification) != REDISMODULE_OK)
        return RedisModule_WrongArity(ctx);
    is_sub = 1;
    return RedisModule_ReplyWithSimpleString(ctx, "OK");
}

int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);
    if (RedisModule_Init(ctx, "notifylist", 1, REDISMODULE_APIVER_1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "notifylist.set",
                                  NotifyListCommand_RedisCommand, "write deny-oom", 1, 1, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    is_sub = 0;
    Keyspace = RedisModule_CreateDict(NULL);
    return REDISMODULE_OK;
}

int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    is_sub = 0;
    RedisModule_FreeDict(ctx, Keyspace);
    return REDISMODULE_OK;
}