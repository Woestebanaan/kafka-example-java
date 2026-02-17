package com.example.kafka;

import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerApp {

    public static void main(String[] args) {
        // Load configuration from application.properties (classpath) and environment variables
        var config = loadConfiguration();

        var bootstrapServers = getConfig(config, "KAFKA_BOOTSTRAP_SERVERS", "Kafka__BootstrapServers", "kafka.bootstrap.servers", "kafka-broker.example.com:9094");
        var groupId = getConfig(config, "KAFKA_GROUP_ID", "Kafka__GroupId", "kafka.group.id", "my-consumer-group");
        var topic = getConfig(config, "KAFKA_TOPIC", "Kafka__Topic", "kafka.topic", "my-topic");
        var autoOffsetReset = getConfig(config, "KAFKA_AUTO_OFFSET_RESET", "Kafka__AutoOffsetReset", "kafka.auto.offset.reset", "earliest");
        var enableAutoCommit = getConfig(config, "KAFKA_ENABLE_AUTO_COMMIT", "Kafka__EnableAutoCommit", "kafka.enable.auto.commit", "true");
        var securityProtocol = getConfig(config, "KAFKA_SECURITY_PROTOCOL", "Kafka__Security__SecurityProtocol", "kafka.security.protocol", "SASL_SSL");
        var saslMechanism = getConfig(config, "KAFKA_SASL_MECHANISM", "Kafka__Security__SaslMechanism", "kafka.sasl.mechanism", "OAUTHBEARER");
        var sslEndpointAlgorithm = getConfig(config, "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "Kafka__Ssl__SslEndpointIdentificationAlgorithm", "kafka.ssl.endpoint.identification.algorithm", "https");
        var sslCaLocation = getConfig(config, "KAFKA_SSL_CA_LOCATION", "Kafka__Ssl__SslCaLocation", "kafka.ssl.ca.location", "");
        var enableInsecureSsl = Boolean.parseBoolean(getConfig(config, "KAFKA_ENABLE_INSECURE_SSL", "Kafka__Ssl__EnableInsecureSsl", "kafka.enable.insecure.ssl", "false"));

        // Get Azure identity configuration
        var clientId = System.getenv("AZURE_CLIENT_ID");
        var tenantId = System.getenv("AZURE_TENANT_ID");
        var clientSecret = System.getenv("AZURE_CLIENT_SECRET");
        var scope = clientId + "/.default";

        // Use ClientSecretCredential when tenant ID and client secret are provided,
        // otherwise fall back to DefaultAzureCredential (Workload Identity, Managed Identity, Azure CLI, etc.)
        var credential = (tenantId != null && !tenantId.isEmpty() && clientSecret != null && !clientSecret.isEmpty())
                ? new ClientSecretCredentialBuilder()
                    .tenantId(tenantId)
                    .clientId(clientId)
                    .clientSecret(clientSecret)
                    .build()
                : new DefaultAzureCredentialBuilder().build();

        // Build Kafka consumer properties
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Security configuration
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", saslMechanism);
        props.put("sasl.login.callback.handler.class", "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler");
        props.put("sasl.oauthbearer.token.endpoint.url", "https://login.microsoftonline.com/");

        // SSL configuration
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, sslEndpointAlgorithm);
        if (!sslCaLocation.isEmpty()) {
            props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslCaLocation);
            if (sslCaLocation.endsWith(".pem")) {
                props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
            }
        }
        if (enableInsecureSsl) {
            props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            props.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, InsecureSslEngineFactory.class.getName());
        }

        // Configure JAAS with Azure OAuth Bearer token provider
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");

        // Override the default login callback with our Azure-based one
        props.put("sasl.login.callback.handler.class", AzureOAuthCallbackHandler.class.getName());

        // Pass Azure credentials via system properties so the callback handler can access them
        AzureOAuthCallbackHandler.setCredential(credential);
        AzureOAuthCallbackHandler.setScope(scope);
        AzureOAuthCallbackHandler.setClientId(clientId);

        var consumer = new KafkaConsumer<String, String>(props);

        // Register shutdown hook for graceful shutdown
        var mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nClosing consumer...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("Subscribed to topic: " + topic);
        System.out.println("Waiting for messages... Press Ctrl+C to exit.");

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("""
                            Received message at %s-%d@%d:
                              Key: %s
                              Value: %s
                              Timestamp: %s
                            %n""",
                            record.topic(), record.partition(), record.offset(),
                            record.key(),
                            record.value(),
                            Instant.ofEpochMilli(record.timestamp()));
                }
            }
        } catch (WakeupException e) {
            // Expected on shutdown
        } finally {
            consumer.close();
            System.out.println("Consumer closed.");
        }
    }

    private static Properties loadConfiguration() {
        var props = new Properties();

        // Load from classpath (application.properties)
        try (InputStream is = KafkaConsumerApp.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            System.out.println("No application.properties found on classpath, using defaults and environment variables.");
        }

        // Load from file in current directory (if exists)
        try (InputStream is = new FileInputStream("application.properties")) {
            props.load(is);
        } catch (IOException e) {
            // Ignore - file doesn't exist
        }

        return props;
    }

    private static String getConfig(Properties props, String envKey, String dotnetEnvKey, String propKey, String defaultValue) {
        // Check JAVA_STYLE env var first
        var envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        // Fall back to .NET-style env var (Kafka__BootstrapServers) for compatibility with C# k8s deployments
        var dotnetEnvValue = System.getenv(dotnetEnvKey);
        if (dotnetEnvValue != null && !dotnetEnvValue.isEmpty()) {
            return dotnetEnvValue;
        }
        return props.getProperty(propKey, defaultValue);
    }
}
