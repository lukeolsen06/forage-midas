package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Component
public class KafkaTransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    private final DatabaseConduit databaseConduit;
    private final TransactionRepository transactionRepository;

    public KafkaTransactionListener(DatabaseConduit databaseConduit, TransactionRepository transactionRepository) {
        this.databaseConduit = databaseConduit;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    @Transactional
    public void receiveTransaction(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Fetch sender and recipient with pessimistic lock to prevent race conditions
        // Lock in ID order to prevent deadlocks
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        Optional<UserRecord> senderOpt, recipientOpt;
        
        if (senderId < recipientId) {
            // Lock sender first, then recipient
            senderOpt = databaseConduit.findUserByIdWithLock(senderId);
            recipientOpt = databaseConduit.findUserByIdWithLock(recipientId);
        } else {
            // Lock recipient first, then sender (because recipient has smaller ID)
            recipientOpt = databaseConduit.findUserByIdWithLock(recipientId);
            senderOpt = databaseConduit.findUserByIdWithLock(senderId);
        }

        // Validate transaction
        if (!isValidTransaction(transaction, senderOpt, recipientOpt)) {
            logger.info("Transaction invalid, discarding: {}", transaction);
            return;
        }

        // Process valid transaction
        UserRecord sender = senderOpt.get();
        UserRecord recipient = recipientOpt.get();

        // Create and save transaction record
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
        transactionRepository.save(transactionRecord);


        // Update balances based on current database state
        float senderNewBalance = sender.getBalance() - transaction.getAmount();
        float recipientNewBalance = recipient.getBalance() + transaction.getAmount();

        databaseConduit.updateUserBalance(sender, senderNewBalance);
        databaseConduit.updateUserBalance(recipient, recipientNewBalance);

        logger.info("Transaction processed successfully: sender {} balance updated to {}, recipient {} balance updated to {}",
                sender.getId(), senderNewBalance, recipient.getId(), recipientNewBalance);
    }

    private boolean isValidTransaction(Transaction transaction, Optional<UserRecord> senderOpt, Optional<UserRecord> recipientOpt) {
        // Validate senderId exists
        if (senderOpt.isEmpty()) {
            logger.debug("Invalid transaction: senderId {} does not exist", transaction.getSenderId());
            return false;
        }

        // Validate recipientId exists
        if (recipientOpt.isEmpty()) {
            logger.debug("Invalid transaction: recipientId {} does not exist", transaction.getRecipientId());
            return false;
        }

        // Validate sender has sufficient balance
        UserRecord sender = senderOpt.get();
        if (sender.getBalance() < transaction.getAmount()) {
            logger.debug("Invalid transaction: sender {} has insufficient balance. Current: {}, Required: {}",
                    sender.getId(), sender.getBalance(), transaction.getAmount());
            return false;
        }

        return true;
    }
}

