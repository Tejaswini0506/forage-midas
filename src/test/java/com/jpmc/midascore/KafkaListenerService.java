package com.jpmc.midascore;

import com.jpmc.midascore.component.IncentiveClient;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.persistence.TransactionRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListenerService {

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRecordRepository transactionRecordRepository;

    @Autowired
    private IncentiveClient incentiveClient;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(String message) {

        String[] parts = message.split(", ");
        long senderId = Long.parseLong(parts[0]);
        long recipientId = Long.parseLong(parts[1]);
        float amount = Float.parseFloat(parts[2]);   // MUST be float

        UserRecord sender = userRepository.findById(senderId);
        UserRecord recipient = userRepository.findById(recipientId);

        // validate users
        if (sender == null || recipient == null) {
            return;
        }

        // validate balance
        if (sender.getBalance() < amount) {
            return;
        }

        // deduct amount from sender
        sender.setBalance(sender.getBalance() - amount);

        // add amount to recipient
        recipient.setBalance(recipient.getBalance() + amount);

        // call incentive API
        Transaction transaction =
                new Transaction(senderId, recipientId, amount);

        float incentive = incentiveClient.getIncentive(transaction);

        // add incentive ONLY (NOT amount again)
        recipient.setBalance(recipient.getBalance() + incentive);

        // save users AFTER all math
        userRepository.save(sender);
        userRepository.save(recipient);

        // save transaction
        TransactionRecord record =
                new TransactionRecord(sender, recipient, amount);

        record.setIncentive(incentive);
        transactionRecordRepository.save(record);
    }
}
