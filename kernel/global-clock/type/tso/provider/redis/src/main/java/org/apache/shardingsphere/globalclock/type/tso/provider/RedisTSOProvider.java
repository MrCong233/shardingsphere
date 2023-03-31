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

package org.apache.shardingsphere.globalclock.type.tso.provider;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Properties;

/**
 * Redis timestamp oracle provider.
 */
@Slf4j
public final class RedisTSOProvider implements TSOProvider {
    
    private static final String TEST_KEY = "test_key";
    
    private static final String TEST_VALUE = "test_value";
    
    private static final String CSN_KEY = "csn";
    
    private static final long ERROR_CSN = 0;
    
    private static final long INIT_CSN = 40000;
    
    private Properties redisTSOProperties;
    
    private JedisPool jedisPool;
    
    @Override
    public void init(final Properties props) {
        if (jedisPool != null) {
            return;
        }
        if (props == null) {
            redisTSOProperties = new Properties();
            RedisTSOProperties.HOST.set(redisTSOProperties, RedisTSOProperties.HOST.getDefaultValue());
            RedisTSOProperties.PORT.set(redisTSOProperties, RedisTSOProperties.PORT.getDefaultValue());
            RedisTSOProperties.PASSWORD.set(redisTSOProperties, RedisTSOProperties.PASSWORD.getDefaultValue());
            RedisTSOProperties.TIMEOUT_INTERVAL.set(redisTSOProperties, RedisTSOProperties.TIMEOUT_INTERVAL.getDefaultValue());
            RedisTSOProperties.MAX_IDLE.set(redisTSOProperties, RedisTSOProperties.MAX_IDLE.getDefaultValue());
            RedisTSOProperties.MAX_TOTAL.set(redisTSOProperties, RedisTSOProperties.MAX_TOTAL.getDefaultValue());
        } else {
            redisTSOProperties = new Properties(props);
            RedisTSOProperties.HOST.set(redisTSOProperties, RedisTSOProperties.HOST.get(props));
            RedisTSOProperties.PORT.set(redisTSOProperties, RedisTSOProperties.PORT.get(props));
            RedisTSOProperties.PASSWORD.set(redisTSOProperties, RedisTSOProperties.PASSWORD.get(props));
            RedisTSOProperties.TIMEOUT_INTERVAL.set(redisTSOProperties, RedisTSOProperties.TIMEOUT_INTERVAL.get(props));
            RedisTSOProperties.MAX_IDLE.set(redisTSOProperties, RedisTSOProperties.MAX_IDLE.get(props));
            RedisTSOProperties.MAX_TOTAL.set(redisTSOProperties, RedisTSOProperties.MAX_TOTAL.get(props));
        }
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(Integer.parseInt(RedisTSOProperties.MAX_IDLE.get(redisTSOProperties)));
        config.setMaxTotal(Integer.parseInt(RedisTSOProperties.MAX_TOTAL.get(redisTSOProperties)));
        if ("".equals(RedisTSOProperties.PASSWORD.get(redisTSOProperties))) {
            jedisPool = new JedisPool(config, RedisTSOProperties.HOST.get(redisTSOProperties),
                    Integer.parseInt(RedisTSOProperties.PORT.get(redisTSOProperties)),
                    Integer.parseInt(RedisTSOProperties.TIMEOUT_INTERVAL.get(redisTSOProperties)));
        } else {
            jedisPool = new JedisPool(config, RedisTSOProperties.HOST.get(redisTSOProperties),
                    Integer.parseInt(RedisTSOProperties.PORT.get(redisTSOProperties)),
                    Integer.parseInt(RedisTSOProperties.TIMEOUT_INTERVAL.get(redisTSOProperties)),
                    RedisTSOProperties.PASSWORD.get(redisTSOProperties));
        }
        if (checkJedisPool()) {
            log.info("Create jedis pool, host: {}, port: {}, maxIdle: {}, maxTotal: {}, timeoutInterval: {}.",
                    RedisTSOProperties.HOST.get(redisTSOProperties), RedisTSOProperties.PORT.get(redisTSOProperties),
                    RedisTSOProperties.MAX_IDLE.get(redisTSOProperties), RedisTSOProperties.MAX_TOTAL.get(redisTSOProperties),
                    RedisTSOProperties.TIMEOUT_INTERVAL.get(redisTSOProperties));
        } else {
            log.error("Create jedis pool failed, host: {}, port: {}, maxIdle: {}, maxTotal: {}, timeoutInterval: {}.",
                    RedisTSOProperties.HOST.get(redisTSOProperties), RedisTSOProperties.PORT.get(redisTSOProperties),
                    RedisTSOProperties.MAX_IDLE.get(redisTSOProperties), RedisTSOProperties.MAX_TOTAL.get(redisTSOProperties),
                    RedisTSOProperties.TIMEOUT_INTERVAL.get(redisTSOProperties));
        }
    }
    
    @Override
    public long getCurrentTimestamp() throws JedisConnectionException {
        Jedis jedis = jedisPool.getResource();
        long result;
        try {
            result = Long.parseLong(jedis.get(CSN_KEY));
        } finally {
            jedis.close();
        }
        if (result == ERROR_CSN) {
            result = initCSN();
        }
        return result;
    }
    
    @Override
    public long getNextTimestamp() throws JedisConnectionException {
        long result;
        try (Jedis jedis = jedisPool.getResource()) {
            result = jedis.incr(CSN_KEY);
        }
        return result;
    }
    
    /**
     * Set csn to INIT_CSN.
     *
     * @return csn
     */
    public synchronized long initCSN() {
        Jedis jedis = jedisPool.getResource();
        String result;
        try {
            result = jedis.set(CSN_KEY, String.valueOf(INIT_CSN));
        } finally {
            jedis.close();
        }
        if ("OK".equals(result)) {
            return INIT_CSN;
        } else {
            return ERROR_CSN;
        }
    }
    
    private boolean checkJedisPool() throws JedisConnectionException {
        String setResult;
        long deleteResult;
        try (Jedis jedis = jedisPool.getResource()) {
            setResult = jedis.set(TEST_KEY, TEST_VALUE);
            deleteResult = jedis.del(TEST_KEY);
        } catch (JedisConnectionException e) {
            log.error("Create jedis pool failed, host: {}, port: {}, maxIdle: {}, maxTotal: {}, timeoutInterval: {}.",
                    RedisTSOProperties.HOST.get(redisTSOProperties), RedisTSOProperties.PORT.get(redisTSOProperties),
                    RedisTSOProperties.MAX_IDLE.get(redisTSOProperties), RedisTSOProperties.MAX_TOTAL.get(redisTSOProperties),
                    RedisTSOProperties.TIMEOUT_INTERVAL.get(redisTSOProperties));
            throw e;
        }
        return "OK".equals(setResult) && deleteResult == 1;
    }
    
    /**
     * Get properties of redisTSOProvider.
     *
     * @return properties
     */
    public Properties getRedisTSOProperties() {
        return redisTSOProperties;
    }
    
    @Override
    public String getType() {
        return "TSO.redis";
    }
}
