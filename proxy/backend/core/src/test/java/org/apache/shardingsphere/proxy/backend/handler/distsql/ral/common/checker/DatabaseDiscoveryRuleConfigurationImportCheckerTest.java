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

package org.apache.shardingsphere.proxy.backend.handler.distsql.ral.common.checker;

import org.apache.shardingsphere.dbdiscovery.api.config.DatabaseDiscoveryRuleConfiguration;
import org.apache.shardingsphere.dbdiscovery.api.config.rule.DatabaseDiscoveryDataSourceRuleConfiguration;
import org.apache.shardingsphere.dbdiscovery.api.config.rule.DatabaseDiscoveryHeartBeatConfiguration;
import org.apache.shardingsphere.distsql.handler.exception.algorithm.MissingRequiredAlgorithmException;
import org.apache.shardingsphere.distsql.handler.exception.storageunit.MissingRequiredStorageUnitsException;
import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.infra.metadata.database.ShardingSphereDatabase;
import org.apache.shardingsphere.infra.util.spi.exception.ServiceProviderNotFoundServerException;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DatabaseDiscoveryRuleConfigurationImportCheckerTest {
    
    private final DatabaseDiscoveryRuleConfigurationImportChecker importChecker = new DatabaseDiscoveryRuleConfigurationImportChecker();
    
    @Test
    void assertCheckDataSources() {
        ShardingSphereDatabase database = mockDatabaseWithDataSource();
        DatabaseDiscoveryRuleConfiguration currentRuleConfig = getRuleConfigWithNotExistedDataSources();
        assertThrows(MissingRequiredStorageUnitsException.class, () -> importChecker.check(database, currentRuleConfig));
    }
    
    @Test
    void assertCheckDiscoveryTypes() {
        ShardingSphereDatabase database = mockDatabase();
        DatabaseDiscoveryRuleConfiguration currentRuleConfig = createRuleConfigWithInvalidDiscoveryType();
        assertThrows(ServiceProviderNotFoundServerException.class, () -> importChecker.check(database, currentRuleConfig));
    }
    
    @Test
    void assertCheckDiscoveryHeartBeats() {
        ShardingSphereDatabase database = mockDatabase();
        DatabaseDiscoveryRuleConfiguration currentRuleConfig = createRuleConfigWithNotExistsHeartBeats();
        assertThrows(MissingRequiredAlgorithmException.class, () -> importChecker.check(database, currentRuleConfig));
    }
    
    private ShardingSphereDatabase mockDatabaseWithDataSource() {
        ShardingSphereDatabase result = mock(ShardingSphereDatabase.class, RETURNS_DEEP_STUBS);
        Collection<String> dataSources = new LinkedList<>();
        dataSources.add("su_1");
        when(result.getResourceMetaData().getNotExistedDataSources(any())).thenReturn(dataSources);
        when(result.getRuleMetaData().getRules()).thenReturn(Collections.emptyList());
        return result;
    }
    
    private DatabaseDiscoveryRuleConfiguration getRuleConfigWithNotExistedDataSources() {
        List<String> dataSourcesNames = new LinkedList<>();
        dataSourcesNames.add("ds_1");
        dataSourcesNames.add("ds_2");
        DatabaseDiscoveryDataSourceRuleConfiguration dataSourceRuleConfig = new DatabaseDiscoveryDataSourceRuleConfiguration("groups", dataSourcesNames, "heart_beat", "type");
        Collection<DatabaseDiscoveryDataSourceRuleConfiguration> dataSources = new LinkedList<>();
        dataSources.add(dataSourceRuleConfig);
        return new DatabaseDiscoveryRuleConfiguration(dataSources, Collections.emptyMap(), Collections.emptyMap());
    }
    
    private ShardingSphereDatabase mockDatabase() {
        ShardingSphereDatabase result = mock(ShardingSphereDatabase.class, RETURNS_DEEP_STUBS);
        when(result.getResourceMetaData().getNotExistedDataSources(any())).thenReturn(Collections.emptyList());
        when(result.getRuleMetaData().getRules()).thenReturn(Collections.emptyList());
        return result;
    }
    
    private DatabaseDiscoveryRuleConfiguration createRuleConfigWithInvalidDiscoveryType() {
        Map<String, AlgorithmConfiguration> discoveryType = new HashMap<>();
        discoveryType.put("invalid_discovery_type", mock(AlgorithmConfiguration.class));
        return new DatabaseDiscoveryRuleConfiguration(mock(Collection.class), mock(Map.class), discoveryType);
    }
    
    private DatabaseDiscoveryRuleConfiguration createRuleConfigWithNotExistsHeartBeats() {
        Map<String, DatabaseDiscoveryHeartBeatConfiguration> heartBeats = new HashMap<>();
        heartBeats.put("heart_beat", mock(DatabaseDiscoveryHeartBeatConfiguration.class));
        List<String> dataSourcesNames = new LinkedList<>();
        dataSourcesNames.add("ds_1");
        dataSourcesNames.add("ds_2");
        DatabaseDiscoveryDataSourceRuleConfiguration dataSourceRuleConfig = new DatabaseDiscoveryDataSourceRuleConfiguration("groups", dataSourcesNames, "heart_beat", "type");
        Collection<DatabaseDiscoveryDataSourceRuleConfiguration> dataSources = new LinkedList<>();
        dataSources.add(dataSourceRuleConfig);
        return new DatabaseDiscoveryRuleConfiguration(dataSources, heartBeats, mock(Map.class));
    }
}
