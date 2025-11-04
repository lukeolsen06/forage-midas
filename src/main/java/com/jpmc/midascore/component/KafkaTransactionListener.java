package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void receiveTransaction(Transaction transaction) {
        // For Task 2, we just need to receive transactions
        // The debugger will be used to inspect the amounts
        logger.debug("Received transaction: {}", transaction);
    }
}

