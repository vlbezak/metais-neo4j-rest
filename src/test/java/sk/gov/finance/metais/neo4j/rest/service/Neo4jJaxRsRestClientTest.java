package sk.gov.finance.metais.neo4j.rest.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.annotation.PostConstruct;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.PropertiesFactoryBean;
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
import sk.gov.finance.metais.neo4j.rest.service.Neo4jTransactionalService;
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
        
        ThreadTestHelper helper = new ThreadTestHelper();
        helper.setResult("bb");
        
        Thread queryThread = new Thread(new QueryThread(helper));
        queryThread.start();
        
        queryThread.join();
        log.info("Joined");
        
        assertEquals("aa", helper.getResult());
       
        
    }
}

class ThreadTestHelper{
    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    String result;
    

}

class QueryThread implements Runnable{

    public QueryThread(ThreadTestHelper result){
        this.result = result;
    }
    
    ThreadTestHelper result;
    
    public void run() {
        System.out.println("QueryThread starting");
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        result.setResult("aa");
        
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
