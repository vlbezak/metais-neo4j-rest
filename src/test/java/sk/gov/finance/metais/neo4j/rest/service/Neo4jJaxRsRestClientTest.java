package sk.gov.finance.metais.neo4j.rest.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import sk.gov.finance.metais.neo4j.rest.bo.CypherData;
import sk.gov.finance.metais.neo4j.rest.bo.CypherResponse;
import sk.gov.finance.metais.neo4j.rest.bo.CypherResult;
import sk.gov.finance.metais.neo4j.rest.bo.Transaction;
import sk.gov.finance.metais.neo4j.rest.service.impl.Neo4jTransactionalServiceJaxRsImpl;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Neo4jRestTestConfig.class)
public class Neo4jJaxRsRestClientTest {

    private static final Logger log = LoggerFactory.getLogger(Neo4jJaxRsRestClientTest.class);

    @Qualifier("jaxRsTestService")
    @Autowired
    Neo4jTransactionalService service;

    @PostConstruct
    public void setupTests() throws Exception {
        log.info("setupTests");
        Transaction trans = service.startTransaction();

        String cypherQuery = "match (testNeo4j:TestNeo4j) delete testNeo4j";
        CypherResponse response = service.runQuery(trans, cypherQuery);

        log.info("CypherResponse:" + response);

        log.info("Transaction:" + trans.getLocationUri() + ":" + trans.getCommitUrl());

        log.info("commitTransaction");
        service.commitTransaction(trans);
    }

    @Test
    public void testTransactionCommit() throws Exception {

        Transaction trans;
        String cypherQuery;
        CypherResponse response;

        // Create object
        //
        log.info("Start transaction");
        trans = service.startTransaction();
        assertNotNull(trans);

        log.info("Create node");
        cypherQuery = "create (testNeo4j:TestNeo4j {name: 'test1', seq: 12, title: 'test'}) return testNeo4j";
        response = service.runQuery(trans, cypherQuery);

        log.info("CypherResponse:" + response);
        assertNotNull(response);
        assertTrue(response.getErrors().size() == 0);

        log.info("Transaction:" + trans.getLocationUri() + ":" + trans.getCommitUrl());
        service.commitTransaction(trans);

        // Check if object exists
        //
        log.info("Start transaction");
        trans = service.startTransaction();
        assertNotNull(trans);

        log.info("Create node");
        cypherQuery = "match (testNeo4j:TestNeo4j {name: 'test1'}) return testNeo4j.name";
        response = service.runQuery(trans, cypherQuery);

        log.info("CypherResponse:" + response);
        assertNotNull(response);
        assertTrue(response.getErrors().size() == 0);

        List<CypherResult> results = response.getResults();
        assertNotNull(results);
        assertTrue("One result should be returned:", results.size() == 1);
        CypherResult result = results.get(0);

        List<CypherData> datas = result.getData();
        List<String> columns = result.getColumns();
        assertNotNull(datas);
        assertTrue(datas.size() == 1);
        assertNotNull(columns);
        assertTrue(columns.size() == 1);
        assertEquals("testNeo4j.name", columns.get(0));

        CypherData data = datas.get(0);
        List<Object> row = data.getRow();
        assertNotNull(row);
        assertTrue(row.size() == 1);
        String value = (String) row.get(0);
        assertEquals("test1", value);

        log.info("Transaction:" + trans.getLocationUri() + ":" + trans.getCommitUrl());
        service.commitTransaction(trans);

    }

    @Test
    public void testTransactionRollback() throws Exception {

        Transaction trans;
        String cypherQuery;
        CypherResponse response;

        // Create object
        //
        log.info("Start transaction");
        trans = service.startTransaction();
        assertNotNull(trans);

        log.info("Create node");
        cypherQuery = "create (testNeo4j:TestNeo4j {name: 'test2', seq: 13, title: 'test'}) return testNeo4j";
        response = service.runQuery(trans, cypherQuery);

        log.info("CypherResponse:" + response);
        assertNotNull(response);
        assertTrue(response.getErrors().size() == 0);

        log.info("Transaction:" + trans.getLocationUri() + ":" + trans.getCommitUrl());
        service.rollbackTransaction(trans);

        // Check if object exists
        //
        log.info("Start transaction");
        trans = service.startTransaction();
        assertNotNull(trans);

        log.info("Create node");
        cypherQuery = "match (testNeo4j:TestNeo4j {name: 'test2'}) return testNeo4j.name";
        response = service.runQuery(trans, cypherQuery);

        log.info("CypherResponse:" + response);
        assertNotNull(response);
        assertTrue(response.getErrors().size() == 0);

        List<CypherResult> results = response.getResults();
        assertNotNull(results);
        assertTrue("One result should be returned:", results.size() == 1);
        CypherResult result = results.get(0);

        // No data should be found
        List<CypherData> datas = result.getData();
        List<String> columns = result.getColumns();
        assertNotNull(datas);
        assertTrue(datas.size() == 0);
        assertNotNull(columns);
        assertTrue(columns.size() == 1);
        assertEquals("testNeo4j.name", columns.get(0));

        log.info("Transaction:" + trans.getLocationUri() + ":" + trans.getCommitUrl());
        service.commitTransaction(trans);

    }


    @Test
    public void testTransactionCommitMultipleThreads() throws Exception {

        Transaction trans;
        String cypherQuery;
        CypherResponse response;

        // Create object
        //
        log.info("Start transaction");
        trans = service.startTransaction();
        assertNotNull(trans);

        log.info("Create node");
        cypherQuery = "create (testNeo4j:TestNeo4j {name: 'test1', seq: 12, title: 'test'}) return testNeo4j";
        response = service.runQuery(trans, cypherQuery);

        log.info("CypherResponse:" + response);
        assertNotNull(response);
        assertTrue(response.getErrors().size() == 0);

        ExecutorService execService = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        
        QueryThreadData td1 = new QueryThreadData(trans, "match (testNeo4j:TestNeo4j {name: 'test1'}) return testNeo4j.name", latch);
        QueryThreadData td2 = new QueryThreadData(trans, "match (testNeo4j:TestNeo4j {name: 'test2'}) return testNeo4j.name", latch);
        
        QueryThread t1 = new QueryThread(td1);
        QueryThread t2 = new QueryThread(td2);

        execService.execute(t1);
        execService.execute(t2);

        latch.await(5, TimeUnit.SECONDS);

        log.info("Executor finished");
        
        execService.shutdown();

        // Check response from first thread - should return data
        //
        assertNotNull(td1.getResponse());
        
        log.info("CypherResponse:" + td1.getResponse());
        assertNotNull(td1.getResponse());
        assertTrue(td1.getResponse().getErrors().size() == 0);

        List<CypherResult> results = td1.getResponse().getResults();
        assertNotNull(results);
        assertTrue("One result should be returned:", results.size() == 1);
        CypherResult result = results.get(0);

        // One row should be found
        List<CypherData> datas = result.getData();
        List<String> columns = result.getColumns();
        assertNotNull(datas);
        assertTrue(datas.size() == 1);
        assertNotNull(columns);
        assertTrue(columns.size() == 1);
        assertEquals("testNeo4j.name", columns.get(0));
        
        CypherData data = datas.get(0);
        List<Object> row = data.getRow();
        assertNotNull(row);
        assertTrue(row.size() == 1);
        String value = (String) row.get(0);
        assertEquals("test1", value);
        
        // Check response from second thread - should not return data
        //
        assertNotNull(td2.getResponse());
        
        log.info("CypherResponse:" + td2.getResponse());
        assertNotNull(td2.getResponse());
        assertTrue(response.getErrors().size() == 0);

        List<CypherResult> results2 = td2.getResponse().getResults();
        assertNotNull(results2);
        assertTrue("One result should be returned:", results2.size() == 1);
        CypherResult result2 = results2.get(0);

        // No row should be found
        List<CypherData> datas2 = result2.getData();
        List<String> columns2 = result2.getColumns();
        assertNotNull(datas2);
        assertTrue(datas2.size() == 0);        
        
        service.commitTransaction(trans);
        
        
        
    }

    class QueryThreadData {
        String query;
        Transaction transaction;
        CountDownLatch latch;
        CypherResponse response; 

        public QueryThreadData(Transaction transaction, String query, CountDownLatch latch) {
            this.transaction = transaction;
            this.query = query;
            this.latch = latch;
        }
        
        public CypherResponse getResponse(){
            return response;
        }

    }

    class QueryThread implements Runnable {

        Neo4jTransactionalService serviceInThread;
        
        public QueryThread(QueryThreadData data) {
            this.data = data;
            
            //Simulate rule module (that will be in separated war with separate ApplicationContext)
            //So create application context manualy
            ApplicationContext context = new AnnotationConfigApplicationContext(Neo4jRestTestConfig.class);

            serviceInThread = context.getBean(Neo4jTransactionalService.class);
        }

        QueryThreadData data;

        public void run() {
            System.out.println("QueryThread starting, service:" + service);

            System.out.print("before query"); 
            CypherResponse response = serviceInThread.runQuery(data.transaction, data.query);
            System.out.println("after query");
                
            System.out.println("Before setting response");
            data.response = response;

            data.latch.countDown();
        }

    }
}

@Configuration
class Neo4jRestTestConfig {

    @Value("#{configProperties['neo4j.url']}")
    String neo4jUrl;

    @Value("#{configProperties['neo4j.username']}")
    String neo4jUsername;

    @Value("#{configProperties['neo4j.password']}")
    String neo4jPassword;

    @Bean(name = "jaxRsTestService")
    Neo4jTransactionalService neo4jRestService() {
        return new Neo4jTransactionalServiceJaxRsImpl(neo4jUrl, neo4jUsername, neo4jPassword);
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
        PropertySourcesPlaceholderConfigurer conf = new PropertySourcesPlaceholderConfigurer();
        return conf;
    }

    @Bean
    public PropertiesFactoryBean configProperties() {
        PropertiesFactoryBean pfBean = new PropertiesFactoryBean();
        pfBean.setLocation(new ClassPathResource("anext-neo4j-rest.test.properties"));
        return pfBean;
    }

}
