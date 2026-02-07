package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class IncentiveClient {

    private final RestTemplate restTemplate = new RestTemplate();

    public float getIncentive(Transaction transaction) {

        IncentiveResponse response =
                restTemplate.postForObject(
                        "http://localhost:8080/incentive",
                        transaction,
                        IncentiveResponse.class
                );

        return response != null ? response.getAmount() : 0f;
    }
}
