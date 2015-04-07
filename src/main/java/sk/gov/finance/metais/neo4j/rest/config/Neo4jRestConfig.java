package sk.gov.finance.metais.neo4j.rest.config;

import java.util.ArrayList;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import sk.gov.finance.metais.neo4j.rest.interceptors.HeaderRequestInterceptor;

@Configuration
public class Neo4jRestConfig {
    
}
