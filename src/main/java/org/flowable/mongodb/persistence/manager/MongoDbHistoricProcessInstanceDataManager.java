/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flowable.mongodb.persistence.manager;

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.flowable.common.engine.impl.persistence.entity.Entity;
import org.flowable.engine.history.HistoricProcessInstance;
import org.flowable.engine.impl.HistoricProcessInstanceQueryImpl;
import org.flowable.engine.impl.persistence.entity.ExecutionEntity;
import org.flowable.engine.impl.persistence.entity.HistoricProcessInstanceEntity;
import org.flowable.engine.impl.persistence.entity.HistoricProcessInstanceEntityImpl;
import org.flowable.engine.impl.persistence.entity.data.HistoricProcessInstanceDataManager;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;
import org.flowable.mongodb.persistence.bean.RelationBean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.flowable.mongodb.persistence.manager.MongoDbProcessDefinitionDataManager.COLLECTION_PROCESS_DEFINITIONS;

/**
 * @author Tijs Rademakers
 */
public class MongoDbHistoricProcessInstanceDataManager extends AbstractMongoDbDataManager<HistoricProcessInstanceEntity> implements HistoricProcessInstanceDataManager {

    public static final String COLLECTION_HISTORIC_PROCESS_INSTANCES = "historicProcessInstances";

    public MongoDbHistoricProcessInstanceDataManager(MongoDbProcessEngineConfiguration processEngineConfiguration) {
        super(processEngineConfiguration);
    }

    @Override
    public String getCollection() {
        return COLLECTION_HISTORIC_PROCESS_INSTANCES;
    }

    @Override
    public HistoricProcessInstanceEntity create() {
        return new HistoricProcessInstanceEntityImpl();
    }

    @Override
    public BasicDBObject createUpdateObject(Entity entity) {
        HistoricProcessInstanceEntity instanceEntity = (HistoricProcessInstanceEntity) entity;
        BasicDBObject updateObject = null;
        updateObject = setUpdateProperty(instanceEntity, "deleteReason", instanceEntity.getDeleteReason(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "endActivityId", instanceEntity.getEndActivityId(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "endTime", instanceEntity.getEndTime(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "startActivityId", instanceEntity.getStartActivityId(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "startTime", instanceEntity.getStartTime(), updateObject);
        updateObject = setUpdateProperty(instanceEntity, "startUserId", instanceEntity.getStartUserId(), updateObject);
        return updateObject;
    }

    @Override
    public HistoricProcessInstanceEntity create(ExecutionEntity processInstanceExecutionEntity) {
        return new HistoricProcessInstanceEntityImpl(processInstanceExecutionEntity);
    }

    @Override
    public List<String> findHistoricProcessInstanceIdsByProcessDefinitionId(String processDefinitionId) {
        List<HistoricProcessInstance> historicProcessInstances = getMongoDbSession().find(COLLECTION_HISTORIC_PROCESS_INSTANCES, Filters.eq("processDefinitionId", processDefinitionId));
        if (historicProcessInstances != null && !historicProcessInstances.isEmpty()) {
            return historicProcessInstances.stream().map(HistoricProcessInstance::getId).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<HistoricProcessInstance> findHistoricProcessInstancesBySuperProcessInstanceId(String superProcessInstanceId) {
        return getMongoDbSession().find(COLLECTION_HISTORIC_PROCESS_INSTANCES, Filters.eq("superProcessInstanceId", superProcessInstanceId));
    }

    @Override
    public List<HistoricProcessInstance> findHistoricProcessInstancesByQueryCriteria(HistoricProcessInstanceQueryImpl historicProcessInstanceQuery) {

        return getMongoDbSession().findAggregates(COLLECTION_HISTORIC_PROCESS_INSTANCES, createFilter(historicProcessInstanceQuery),
                new RelationBean(COLLECTION_PROCESS_DEFINITIONS, "processDefinitionId", "_id", "processDefinition"));
    }

    @Override
    public long findHistoricProcessInstanceCountByQueryCriteria(HistoricProcessInstanceQueryImpl historicProcessInstanceQuery) {
        return getMongoDbSession().count(COLLECTION_HISTORIC_PROCESS_INSTANCES, createFilter(historicProcessInstanceQuery));
    }

    @Override
    public List<HistoricProcessInstance> findHistoricProcessInstancesAndVariablesByQueryCriteria(HistoricProcessInstanceQueryImpl historicProcessInstanceQuery) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<HistoricProcessInstance> findHistoricProcessInstancesByNativeQuery(Map<String, Object> parameterMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long findHistoricProcessInstanceCountByNativeQuery(Map<String, Object> parameterMap) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteHistoricProcessInstances(HistoricProcessInstanceQueryImpl historicProcessInstanceQuery) {
        throw new UnsupportedOperationException();
    }

    protected Bson createFilter(HistoricProcessInstanceQueryImpl processInstanceQuery) {
        List<Bson> andFilters = new ArrayList<>();
        if(processInstanceQuery.getStartedBy() != null){
            andFilters.add(Filters.eq("startUserId", processInstanceQuery.getStartedBy()));
        }

        if (processInstanceQuery.getProcessInstanceId() != null) {
            andFilters.add(Filters.eq("processInstanceId", processInstanceQuery.getProcessInstanceId()));
        }

        if (processInstanceQuery.getDeploymentId() != null) {
            andFilters.add(Filters.eq("deploymentId", processInstanceQuery.getDeploymentId()));
        }

        if (processInstanceQuery.getProcessDefinitionId() != null) {
            andFilters.add(Filters.eq("processDefinitionId", processInstanceQuery.getProcessDefinitionId()));
        }

        if (processInstanceQuery.getSuperProcessInstanceId() != null) {
            andFilters.add(Filters.eq("superProcessInstanceId", processInstanceQuery.getSuperProcessInstanceId()));
        }

        Bson filter = null;
        if (andFilters.size() > 0) {
            filter = Filters.and(andFilters.toArray(new Bson[andFilters.size()]));
        }

        return filter;
    }

}
