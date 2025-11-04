package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveService {
    private static final Logger logger = LoggerFactory.getLogger(IncentiveService.class);
    private static final String INCENTIVE_API_URL = "http://localhost:8080/incentive";
    
    private final RestTemplate restTemplate;

    public IncentiveService(RestTemplateBuilder builder) {
        this.restTemplate = builder.build();
    }

    public Incentive getIncentive(Transaction transaction) {
        logger.debug("Calling incentive API for transaction: {}", transaction);
        try {
            Incentive incentive = restTemplate.postForObject(INCENTIVE_API_URL, transaction, Incentive.class);
            logger.debug("Received incentive: {}", incentive != null ? incentive.getAmount() : "null");
            return incentive;
        } catch (Exception e) {
            logger.warn("Error calling incentive API: {}, defaulting to 0.0", e.getMessage());
            return null; // Return null if API call fails, will default to 0.0f
        }
    }
}

