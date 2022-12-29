package org.apache.shardingsphere.globallogicaltime.redis.connector;

/**
 * JedisPool config params for global logical time.
 */
public class GlobalLogicalTimeJedisPoolConfigParams {

    /* the key of csn saved in redis server */
    public static String CSN_KEY = "csn";

    public static String CSN_LOCK_NAME = "csnLock";

    /* init csn when redis server doesn't store csnKey */
    public static long INIT_CSN = 40000;

    /* error csn */
    public static long ERROR_CSN = 0;

    /* default host of redis server */
    public static String DEFAULT_HOST = "127.0.0.1";

    /* default port of redis server */
    public static int DEFAULT_PORT = 6379;

    /* default password of redis server */
    public static String DEFAULT_PASSWORD = "";

    /* default timeout interval when get connection */
    public static int DEFAULT_TIME_INTERVAL = 40000;

    /* default max idle redis connection number */
    public static int DEFAULT_MAX_IDLE = 8;

    /* default max redis connection number */
    public static int DEFAULT_MAX_TOTAL = 18;

    /* the default expiration time of csn lock, redis will keep the csn lock in lockExpirationTime */
    public static int DEFAULT_LOCK_EXPIRATION_TIME = 20;

    /* the min expiration time of csn lock */
    public static int MIN_LOCK_EXPIRATION_TIME = 1;

    /* the max expiration time of csn lock */
    public static int MAX_LOCK_EXPIRATION_TIME = 1000;
}
