package com.example.kafka;

import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaProducerApp {

    public static void main(String[] args) {
        // Load configuration from application.properties (classpath) and environment variables
        var config = loadConfiguration();

        var bootstrapServers = getConfig(config, "KAFKA_BOOTSTRAP_SERVERS", "Kafka__BootstrapServers", "kafka.bootstrap.servers", "kafka-broker.example.com:9094");
        var topic = getConfig(config, "KAFKA_TOPIC", "Kafka__Topic", "kafka.topic", "my-topic");
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

        // Build Kafka producer properties
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

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

        var producer = new KafkaProducer<String, String>(props);

        // Register shutdown hook for graceful shutdown
        var running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nClosing producer...");
            running.set(false);
        }));

        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            hostname = "unknown";
        }

        System.out.println("Producing to topic: " + topic);
        System.out.println("Publishing current time every second... Press Ctrl+C to exit.");

        try {
            while (running.get()) {
                var message = Instant.now().toString();
                var record = new ProducerRecord<>(topic, hostname, message);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.out.println("Produce error: " + exception.getMessage());
                    } else {
                        System.out.printf("Produced message to %s-%d@%d: %s%n",
                                metadata.topic(), metadata.partition(), metadata.offset(), message);
                    }
                });

                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            // Expected on shutdown
        } finally {
            producer.flush();
            producer.close();
            System.out.println("Producer closed.");
        }
    }

    private static Properties loadConfiguration() {
        var props = new Properties();

        // Load from classpath (application.properties)
        try (InputStream is = KafkaProducerApp.class.getClassLoader().getResourceAsStream("application.properties")) {
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
