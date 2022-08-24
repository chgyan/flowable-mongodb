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
package org.flowable.test;

import java.util.Arrays;
import java.util.Collection;

import org.bson.Document;
import org.flowable.engine.HistoryService;
import org.flowable.engine.ManagementService;
import org.flowable.engine.ProcessEngine;
import org.flowable.engine.RepositoryService;
import org.flowable.engine.RuntimeService;
import org.flowable.engine.TaskService;
import org.flowable.engine.impl.test.TestHelper;
import org.flowable.engine.runtime.ProcessInstance;
import org.flowable.job.service.impl.asyncexecutor.DefaultAsyncJobExecutor;
import org.flowable.mongodb.cfg.MongoDbProcessEngineConfiguration;
import org.flowable.mongodb.persistence.MongoDbSession;
import org.flowable.mongodb.persistence.MongoDbSessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.opentest4j.AssertionFailedError;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;

/**
 * @author Tijs Rademakers
 * @author Joram Barrez
 */
public class AbstractMongoDbTest {
    
    protected MongoDbProcessEngineConfiguration processEngineConfiguration;
    protected ProcessEngine processEngine;
    protected RepositoryService repositoryService;
    protected RuntimeService runtimeService;
    protected TaskService taskService;
    protected HistoryService historyService;
    protected ManagementService managementService;

    protected String deploymentId;
    
    @BeforeEach
    public void setup(TestInfo testInfo) {
        if (this.processEngineConfiguration == null) {
            initProcessEngine();
        }

        this.deploymentId = TestHelper.annotationDeploymentSetUp(processEngine, getClass(), testInfo.getTestMethod().get().getName());
    }

    protected void initProcessEngine() {
        this.processEngineConfiguration = (MongoDbProcessEngineConfiguration) new MongoDbProcessEngineConfiguration().setUri("mongodb://127.0.0.1:27017")
//                .setServerAddresses(Arrays.asList(new ServerAddress("localhost", 27017)))
                .setDisableIdmEngine(true);
        
        DefaultAsyncJobExecutor asyncJobExecutor = new DefaultAsyncJobExecutor();
        asyncJobExecutor.setDefaultAsyncJobAcquireWaitTimeInMillis(1000);
        asyncJobExecutor.setDefaultTimerJobAcquireWaitTimeInMillis(1000);
        processEngineConfiguration.setAsyncExecutor(asyncJobExecutor);
        processEngineConfiguration.setAsyncFailedJobWaitTime(1);
        
        this.processEngine = processEngineConfiguration.buildProcessEngine();
        this.repositoryService = this.processEngine.getRepositoryService();
        this.runtimeService = this.processEngine.getRuntimeService();
        this.taskService = this.processEngine.getTaskService();
        this.historyService = this.processEngine.getHistoryService();
        this.managementService = this.processEngine.getManagementService();
    }
    
    @AfterEach
    public void cleanup() {
        deleteAllDocuments();
    }

    protected void deleteAllDocuments() {
        MongoDbSessionFactory mongoDbSessionFactory = (MongoDbSessionFactory) processEngineConfiguration.getSessionFactories().get(MongoDbSession.class);
        Collection<String> collectionNames = mongoDbSessionFactory.getCollectionNames();
        for (String collectionName :collectionNames) {
            MongoCollection<Document> collection = processEngineConfiguration.getMongoDatabase().getCollection(collectionName);
            if (collection != null) {
                collection.deleteMany(new BasicDBObject());
            }
        }
    }

    protected void assertProcessEnded(final String processInstanceId) {
        ProcessInstance processInstance = processEngine.getRuntimeService().createProcessInstanceQuery().processInstanceId(processInstanceId).singleResult();

        if (processInstance != null) {
            throw new AssertionFailedError("Expected finished process instance '" + processInstanceId + "' but it was still in the db");
        }
    }
}
