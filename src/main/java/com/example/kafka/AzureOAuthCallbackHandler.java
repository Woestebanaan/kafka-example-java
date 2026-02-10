package com.example.kafka;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Kafka SASL/OAUTHBEARER callback handler that obtains tokens from Azure Entra ID
 * using DefaultAzureCredential (supports Workload Identity, Managed Identity, Azure CLI, etc.).
 */
public class AzureOAuthCallbackHandler implements AuthenticateCallbackHandler {

    private static volatile TokenCredential credential;
    private static volatile String scope;
    private static volatile String clientId;

    public static void setCredential(TokenCredential cred) {
        credential = cred;
    }

    public static void setScope(String s) {
        scope = s;
    }

    public static void setClientId(String id) {
        clientId = id;
    }

    @Override
    public void configure(Map<String, ?> configs, String saslMechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        // Configuration is passed via static setters before consumer creation
    }

    @Override
    public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof OAuthBearerTokenCallback oauthCallback) {
                try {
                    var tokenRequestContext = new TokenRequestContext().addScopes(scope);
                    var accessToken = credential.getToken(tokenRequestContext).block();

                    if (accessToken == null) {
                        oauthCallback.error("token_acquisition_failed", "Failed to acquire token from Azure Entra ID", null);
                        return;
                    }

                    oauthCallback.token(new OAuthBearerToken() {
                        @Override
                        public String value() {
                            return accessToken.getToken();
                        }

                        @Override
                        public Long startTimeMs() {
                            return System.currentTimeMillis();
                        }

                        @Override
                        public long lifetimeMs() {
                            return accessToken.getExpiresAt().toInstant().toEpochMilli();
                        }

                        @Override
                        public Set<String> scope() {
                            return Collections.singleton(scope);
                        }

                        @Override
                        public String principalName() {
                            return clientId;
                        }
                    });
                } catch (Exception ex) {
                    oauthCallback.error("token_acquisition_failed", ex.getMessage(), null);
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}
