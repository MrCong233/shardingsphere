/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.transaction.xa.glt;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * maintain CSN using Redis
 */
@Slf4j
public class RedisConnector implements GLTConnector, JedisPoolConfigParams {
    
    // the expiration time of redis lock
    private int lockExpirationTime;
    
    private JedisPool jedisPool;
    
    private static final String CSN_LOCK_NAME = "csnLock";
    /* return 1 means redis unlock the csnLock successfully */
    private static final Long RELEASE_SUCCESS = 1L;
    
    /* the timeout interval each time trying to get csn lock */
    private static final int retryIntervalTime = 1000;
    
    /* the max timeout interval where trying to get csn lock */
    private static final int tryLockTimeoutInterval = 40;
    
    private static final Random retryTImeIntervalRandom = new Random();
    
    /**
     * load configuration in redis.properties by path
     * and init jedis pool
     *
     * @param path the path of redis.properties
     * @return if init jedis pool successfully
     */
    private boolean initJedisPool(String path) {
        
        String host;
        int port;
        int timeoutInterval;
        int maxIdle;
        int maxTotal;
        String password;
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        InputStream resourceAsStream = loader.getResourceAsStream(path);
        if (resourceAsStream == null) {
            return false;
        }
        Properties p = new Properties();
        try {
            p.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        
        /* load data */
        try {
            host = p.getProperty("host", DEFAULT_HOST);
            port = Integer.parseInt(p.getProperty("port", DEFAULT_PORT));
            timeoutInterval = Integer.parseInt(p.getProperty("timeoutInterval", DEFAULT_TIME_INTERVAL));
            maxIdle = Integer.parseInt(p.getProperty("maxIdle", DEFAULT_MAX_IDLE));
            maxTotal = Integer.parseInt(p.getProperty("maxTotal", DEFAULT_MAX_TOTAL));
            password = p.getProperty("password", DEFAULT_PASSWORD);
            this.lockExpirationTime = Integer.parseInt(p.getProperty("lockExpirationTime", DEFAULT_LOCK_EXPIRATION_TIME));
            this.lockExpirationTime = Math.max(this.lockExpirationTime, MIN_LOCK_EXPIRATION_TIME);
            this.lockExpirationTime = Math.min(this.lockExpirationTime, MAX_LOCK_EXPIRATION_TIME);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return false;
        }
        
        /* build a jedisPool based on above configuration */
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(maxIdle);
        config.setMaxTotal(maxTotal);
        if (password.equals("")) {
            jedisPool = new JedisPool(config, host, port, timeoutInterval);
        } else {
            jedisPool = new JedisPool(config, host, port, timeoutInterval, password);
        }
        log.info("create jedis pool, host: {}, port: {}, maxIdle: {}, maxTotal: {}, timeoutInterval: {}", host, port, maxIdle, maxTotal, timeoutInterval);
        return true;
    }
    
    private RedisConnector() {
        /* preferentially use external profile, if not exit, use internal profile. */
        if (!initJedisPool(redisExternalProfilePath)) {
            initJedisPool(redisInternalProfilePath);
        }
    }
    
    private static class RedisConnectorHolder {
        
        private static final RedisConnector INSTANCE = new RedisConnector();
    }
    
    /**
     * get singleton instance of RedisConnector
     *
     * @return instance
     */
    public static RedisConnector getInstance() {
        return RedisConnectorHolder.INSTANCE;
    }
    
    /**
     * get current global csn from Redis, and add 1 to the global csn in Redis
     *
     * @return the global csn before add 1
     */
    @Override
    synchronized public long gltGetNextCSN() {
        Jedis jedis = jedisPool.getResource();
        long csn;
        try {
            csn = jedis.incr(csnKey);
        } finally {
            jedis.close();
        }
        return csn - 1;
    }
    
    /**
     * init global csn in redis
     *
     * @return initialized global csn
     */
    @Override
    synchronized public long gltInitCSN() {
        Jedis jedis = jedisPool.getResource();
        String result;
        try {
            result = jedis.set(csnKey, String.valueOf(INIT_CSN));
        } finally {
            jedis.close();
        }
        if (result.equals("OK")) {
            return INIT_CSN;
        } else {
            return ERROR_CSN;
        }
    }
    
    /**
     * get current global csn from redis
     *
     * @return current global csn
     */
    @Override
    public long gltGetCurrentCSN() {
        Jedis jedis = jedisPool.getResource();
        long csn;
        try {
            csn = Long.parseLong(jedis.get(csnKey));
        } finally {
            jedis.close();
        }
        if (csn == ERROR_CSN) {
            csn = gltInitCSN();
        }
        return csn;
    }
    
    private boolean gltTryCSNLock(String id) {
        return gltTryCSNLock(id, lockExpirationTime);
    }
    
    synchronized private boolean gltTryCSNLock(String id, int expireTime) {
        Jedis jedis = jedisPool.getResource();
        SetParams params = new SetParams();
        params.ex(expireTime);
        params.nx();
        String back = jedis.set(CSN_LOCK_NAME, id, params);
        jedis.close();
        return "OK".equals(back);
    }
    
    private boolean isTimeout(long startTime, long endTime) {
        final double msToS = 1000.0;
        double interval = (endTime - startTime) / msToS;
        return !(interval < tryLockTimeoutInterval);
    }
    
    /**
     * try csn lock from redis
     *
     * @param  id cskLock id
     * @throws TimeoutException trying csn lock timeout
     */
    public void gltLockCSN(String id) throws TimeoutException {
        final int MIN_RETRY_TIME_INTERVAL = 50;
        final int MAX_RETRY_TIME_INTERVAL = 100;
        boolean lockResult = false;
        try {
            long startTime = System.currentTimeMillis();
            while (true) {
                long endTime = System.currentTimeMillis();
                if (isTimeout(startTime, endTime)) {
                    break;
                }
                lockResult = gltTryCSNLock(id);
                if (lockResult) {
                    log.info("get csn lock successfully, id : {}", id);
                    break;
                } else {
                    Thread.sleep(retryTImeIntervalRandom.nextInt(MAX_RETRY_TIME_INTERVAL - MIN_RETRY_TIME_INTERVAL) + MIN_RETRY_TIME_INTERVAL);
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        if (!lockResult) {
            throw new TimeoutException("Get lock failed because of timeout.");
        }
    }
    
    /**
     * unlock csnLock if the held id equal to the saved id
     *
     * @param id csn lock id
     * @return if csn lock is unlocked successfully
     */
    public boolean gltUnLockCSN(String id) {
        Jedis jedis = jedisPool.getResource();
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedis.eval(script, Collections.singletonList(CSN_LOCK_NAME), Collections.singletonList(id));
        jedis.close();
        if (RELEASE_SUCCESS.equals(result)) {
            log.info("release csn lock successfully, id : {}", id);
            return true;
        } else {
            log.error("fail to unLock CSN.");
            return false;
        }
    }
    
    /* generate a unique id for csnLock */
    
    /**
     * calculate a csn lock id by process id and timeMillis
     *
     * @return csn lock id
     */
    public static String getTryLockId() {
        
        // get process id
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String processId = String.valueOf(Integer.valueOf(runtimeMXBean.getName().split("@")[0]).intValue());
        // get timeMillis
        String timeMillis = String.valueOf(System.currentTimeMillis());
        String seed = String.valueOf(new Random().nextInt(Integer.MAX_VALUE));
        return processId + timeMillis + seed;
    }
}