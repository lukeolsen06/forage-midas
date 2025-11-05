package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class StressTest {
    static final Logger logger = LoggerFactory.getLogger(StressTest.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private DatabaseConduit databaseConduit;

    @Autowired
    private TransactionRepository transactionRepository;

    @Test
    void stress_test_1000_transactions() throws InterruptedException {
        logger.info("=== Starting Stress Test with 1000 Transactions ===");
        
        // Populate users with starting balances
        userPopulator.populate();
        
        // Load and store starting balances for verification
        Map<Long, Float> startingBalances = new HashMap<>();
        for (int i = 0; i < 12; i++) {
            databaseConduit.findUserById((long) i).ifPresent(user -> {
                startingBalances.put(user.getId(), user.getBalance());
                logger.debug("User {} ({}) starting balance: {}", user.getId(), user.getName(), user.getBalance());
            });
        }

        // Send all 1000 transactions
        logger.info("Sending 1000 transactions to Kafka...");
        String[] transactionLines = fileLoader.loadStrings("/test_data/stress_test_1000.txt");
        long startTime = System.currentTimeMillis();
        
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        
        logger.info("All transactions sent. Waiting for processing...");
        
        // Wait for all transactions to be processed (longer wait for 1000 transactions)
        Thread.sleep(10000);
        
        long endTime = System.currentTimeMillis();
        long processingTime = endTime - startTime;
        logger.info("Processing completed in {} ms ({} seconds)", processingTime, processingTime / 1000.0);

        // Calculate expected balances from processed transactions
        Map<Long, Float> expectedBalances = new HashMap<>(startingBalances);
        
        logger.info("Calculating expected balances from processed transactions...");
        Iterable<TransactionRecord> allTransactions = transactionRepository.findAll();
        int processedCount = 0;
        
        for (TransactionRecord tx : allTransactions) {
            processedCount++;
            Long senderId = tx.getSender().getId();
            Long recipientId = tx.getRecipient().getId();
            float amount = tx.getAmount();
            float incentive = tx.getIncentive();
            
            // Deduct from sender
            expectedBalances.put(senderId, expectedBalances.get(senderId) - amount);
            
            // Add to recipient (amount + incentive)
            expectedBalances.put(recipientId, expectedBalances.get(recipientId) + amount + incentive);
        }
        
        logger.info("Processed {} valid transactions", processedCount);
        logger.info("Expected {} transactions (some may have been rejected due to insufficient balance)", transactionLines.length);

        // Verify actual balances match expected
        logger.info("\n=== Balance Verification ===");
        final boolean[] allBalancesMatch = {true};
        final float[] totalSystemBalance = {0.0f};
        final float[] expectedTotalSystemBalance = {0.0f};
        
        for (int i = 0; i < 12; i++) {
            databaseConduit.findUserById((long) i).ifPresent(user -> {
                Long userId = user.getId();
                float actualBalance = user.getBalance();
                float expectedBalance = expectedBalances.getOrDefault(userId, 0.0f);
                float startingBalance = startingBalances.getOrDefault(userId, 0.0f);
                
                totalSystemBalance[0] += actualBalance;
                expectedTotalSystemBalance[0] += expectedBalance;
                
                boolean matches = Math.abs(actualBalance - expectedBalance) < 0.01f; // Allow small floating point differences
                
                if (!matches) {
                    allBalancesMatch[0] = false;
                    logger.error("MISMATCH for User {} ({}): Expected={}, Actual={}, Starting={}", 
                            userId, user.getName(), expectedBalance, actualBalance, startingBalance);
                } else {
                    logger.info("✓ User {} ({}): Expected={}, Actual={}, Starting={}", 
                            userId, user.getName(), expectedBalance, actualBalance, startingBalance);
                }
            });
        }
        
        // Verify system balance (total should increase by sum of all incentives)
        logger.info("\n=== System Balance Verification ===");
        logger.info("Expected Total System Balance: {}", expectedTotalSystemBalance[0]);
        logger.info("Actual Total System Balance: {}", totalSystemBalance[0]);
        logger.info("Difference: {}", totalSystemBalance[0] - expectedTotalSystemBalance[0]);
        
        // Calculate total incentives injected
        float totalIncentives = 0.0f;
        for (TransactionRecord tx : allTransactions) {
            totalIncentives += tx.getIncentive();
        }
        logger.info("Total Incentives Applied: {}", totalIncentives);
        
        float startingTotal = startingBalances.values().stream().reduce(0.0f, Float::sum);
        logger.info("Starting Total Balance: {}", startingTotal);
        logger.info("Expected Final Total (starting + incentives): {}", startingTotal + totalIncentives);
        logger.info("Actual Final Total: {}", totalSystemBalance[0]);
        
        boolean systemBalanceMatches = Math.abs(totalSystemBalance[0] - (startingTotal + totalIncentives)) < 0.01f;
        if (systemBalanceMatches) {
            logger.info("✓ System balance integrity verified: Total increased by incentives only");
        } else {
            logger.error("✗ System balance integrity FAILED");
        }
        
        logger.info("\n=== Stress Test Summary ===");
        logger.info("Transactions sent: {}", transactionLines.length);
        logger.info("Transactions processed: {}", processedCount);
        logger.info("Transactions rejected: {}", transactionLines.length - processedCount);
        logger.info("Processing time: {} ms ({} seconds)", processingTime, processingTime / 1000.0);
        logger.info("Average processing time per transaction: {} ms", 
                processedCount > 0 ? (double) processingTime / processedCount : 0);
        
        if (allBalancesMatch[0] && systemBalanceMatches) {
            logger.info("✓ STRESS TEST PASSED: All balances match expected values and system integrity maintained");
        } else {
            logger.error("✗ STRESS TEST FAILED: Balance mismatches detected");
        }
    }
}

