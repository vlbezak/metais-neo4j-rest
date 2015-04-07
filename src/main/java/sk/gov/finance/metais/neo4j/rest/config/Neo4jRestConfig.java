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
    @Bean(name="neo4jRestTemplate")
    RestTemplate neo4jRestTemplate() {
            RestTemplate restTemplate = new RestTemplate();
            restTemplate.getMessageConverters().add(
                            new MappingJackson2HttpMessageConverter());
            restTemplate.getMessageConverters().add(
                            new StringHttpMessageConverter());
            
            List<ClientHttpRequestInterceptor> interceptors = restTemplate.getInterceptors();
            
            if(interceptors == null)
            {
                    interceptors = new ArrayList<ClientHttpRequestInterceptor>();
            }
            
            interceptors.add(new HeaderRequestInterceptor("Authorization", "Basic bmVvNGo6emFxMTIz"));
            interceptors.add(new HeaderRequestInterceptor("Accept", "application/json; charset=UTF-8"));
            interceptors.add(new HeaderRequestInterceptor("Content-Type", "application/json"));
            
            return restTemplate;
    }
}
