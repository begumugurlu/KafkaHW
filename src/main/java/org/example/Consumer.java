package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Properties;
import java.util.Collections;

public class Consumer {
    private static final String TOPIC = "operations";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "consumer-group";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        ObjectMapper mapper = new ObjectMapper();

        while (true) {
            for (ConsumerRecord<String, String> record : consumer.poll(100)) {
                OperationMessage message = mapper.readValue(record.value(), OperationMessage.class);
                int operand = message.getOperand();
                String operation = message.getOperation();

                if ("fibPrime".equals(operation)) {
                    boolean isFib = isFibonacci(operand);
                    boolean isPrime = isPrime(operand);

                    if (isFib && isPrime) {
                        System.out.println(operand + " is both Fibonacci and Prime.");
                    } else if (isFib) {
                        System.out.println(operand + " is Fibonacci.");
                    } else if (isPrime) {
                        System.out.println(operand + " is Prime.");
                    } else {
                        System.out.println(operand + " is neither Fibonacci nor Prime.");
                    }
                } else {
                    System.out.println("Error: Invalid operation " + operation);
                }
            }
        }
    }

    private static boolean isFibonacci(int n) {
        int a = 0, b = 1, c;
        if (n == a || n == b) return true;
        while (b < n) {
            c = a + b;
            a = b;
            b = c;
            if (b == n) return true;
        }
        return false;
    }

    private static boolean isPrime(int n) {
        if (n <= 1) return false;
        for (int i = 2; i <= Math.sqrt(n); i++) {
            if (n % i == 0) return false;
        }
        return true;
    }
}

