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

package org.apache.shardingsphere.globallogicaltime.rule.builder;

import org.apache.shardingsphere.globallogicaltime.config.GlobalLogicalTimeRuleConfiguration;
import org.apache.shardingsphere.globallogicaltime.config.RedisConnectionOptionConfiguration;
import org.apache.shardingsphere.globallogicaltime.constant.GlobalLogicalTimeOrder;
import org.apache.shardingsphere.infra.rule.builder.global.DefaultGlobalRuleConfigurationBuilder;

/**
 * Default global logical time rule configuration builder.
 */
public class DefaultGlobalLogicalTimeRuleConfigurationBuilder implements DefaultGlobalRuleConfigurationBuilder<GlobalLogicalTimeRuleConfiguration, GlobalLogicalTimeRuleBuilder> {
    
    public static final RedisConnectionOptionConfiguration REDIS_CONNECTION_OPTION = new RedisConnectionOptionConfiguration(
            "127.0.0.1", "6379", "", 40000,
            8, 18, 10);
    
    @Override
    public GlobalLogicalTimeRuleConfiguration build() {
        return new GlobalLogicalTimeRuleConfiguration(false, REDIS_CONNECTION_OPTION);
    }
    
    @Override
    public int getOrder() {
        return GlobalLogicalTimeOrder.ORDER;
    }
    
    @Override
    public Class<GlobalLogicalTimeRuleBuilder> getTypeClass() {
        return GlobalLogicalTimeRuleBuilder.class;
    }
}
