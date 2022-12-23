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

public interface JedisPoolConfigParams {
    
    /* the key of csn saved in redis server */
    String csnKey = "csn";
    
    /* init csn when redis server doesn't store csnKey */
    long INIT_CSN = 40000;
    
    /* error csn */
    long ERROR_CSN = 0;
    
    /* default host of redis server */
    String DEFAULT_HOST = "127.0.0.1";
    
    /* default port of redis server */
    String DEFAULT_PORT = "6379";
    
    /* default password of redis server */
    String DEFAULT_PASSWORD = "";
    
    /* default timeout interval when get connection */
    String DEFAULT_TIME_INTERVAL = "40000";
    
    /* default max idle redis connection number */
    String DEFAULT_MAX_IDLE = "8";
    
    /* default max redis connection number */
    String DEFAULT_MAX_TOTAL = "18";
    
    /* the default expiration time of csn lock, redis will keep the csn lock in lockExpirationTime */
    String DEFAULT_LOCK_EXPIRATION_TIME = "20";
    
    /* the min expiration time of csn lock */
    int MIN_LOCK_EXPIRATION_TIME = 1;
    
    /* the max expiration time of csn lock */
    int MAX_LOCK_EXPIRATION_TIME = 1000;
    
    /* the external profile path of redis.properties, we preferentially use it if. */
    String redisExternalProfilePath = "././conf/redis.properties";
    
    /* the internal profile path of redis.properties, we will use it if external profile not exist. */
    String redisInternalProfilePath = "redis.properties";
}
