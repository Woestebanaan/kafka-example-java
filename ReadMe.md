# Kafka Consumer Example (Java)

A Java 21 Kafka consumer that uses Azure Entra ID (formerly Azure AD) with federated credentials for authentication.

## Prerequisites

- Java 21 (or later)
- Maven 3.9+
- Azure CLI (for local development)
- Access to an Azure Entra ID tenant

## Configuration

### application.properties

```properties
kafka.bootstrap.servers=your-kafka-broker:9094
kafka.group.id=your-consumer-group
kafka.topic=your-topic
kafka.auto.offset.reset=earliest
kafka.enable.auto.commit=true
kafka.security.protocol=SASL_SSL
kafka.sasl.mechanism=OAUTHBEARER
kafka.ssl.endpoint.identification.algorithm=https
kafka.ssl.ca.location=ca-kafka.pem
kafka.enable.insecure.ssl=false
```

All properties can be overridden with environment variables using uppercase and underscores (e.g., `KAFKA_BOOTSTRAP_SERVERS`).

## Configuring Azure Entra ID

### Step 1: Create an App Registration

1. Go to the [Azure Portal](https://portal.azure.com)
2. Navigate to **Microsoft Entra ID** > **App registrations**
3. Click **New registration**
4. Enter a name for your application (e.g., `kafka-consumer`)
5. Select the appropriate account type (typically "Accounts in this organizational directory only")
6. Click **Register**
7. Note the **Application (client) ID** - you'll need this for `AZURE_CLIENT_ID`

### Step 2: Configure Federated Credentials

Federated credentials allow your application to authenticate without secrets by trusting tokens from an external identity provider (like Kubernetes).

1. In your App Registration, go to **Certificates & secrets**
2. Select the **Federated credentials** tab
3. Click **Add credential**
4. Select the scenario:

#### For Azure Kubernetes Service (AKS):

1. Select **Kubernetes accessing Azure resources**
2. Fill in:
   - **Cluster issuer URL**: Your AKS OIDC issuer URL (find it with `az aks show --name <aks-name> --resource-group <rg> --query "oidcIssuerProfile.issuerUrl" -o tsv`)
   - **Namespace**: The Kubernetes namespace where your app runs
   - **Service account**: The Kubernetes service account name
   - **Name**: A descriptive name for this credential
3. Click **Add**

#### For GitHub Actions:

1. Select **GitHub Actions deploying Azure resources**
2. Fill in:
   - **Organization**: Your GitHub organization or username
   - **Repository**: Your repository name
   - **Entity type**: Branch, Tag, or Environment
   - **GitHub entity name**: The branch/tag/environment name
3. Click **Add**

#### For Other Identity Providers:

1. Select **Other issuer**
2. Fill in:
   - **Issuer**: The OIDC issuer URL
   - **Subject identifier**: The subject claim value
   - **Name**: A descriptive name
3. Click **Add**

### Step 3: Grant Kafka Permissions

Ensure your App Registration has the necessary permissions to access Kafka:

1. Contact your Kafka administrator to add your App Registration's Client ID to the appropriate ACLs
2. The scope in your configuration should be `<client-id>/.default`

## Running the Application

### Local Development

For local development, authenticate using Azure CLI:

```bash
# Login to Azure
az login

# Verify you're logged in
az account show

# Set the Azure Client ID
export AZURE_CLIENT_ID=<your-app-registration-client-id>

# Build and run
mvn clean package -DskipTests
java -jar target/kafka-example-java-1.0-SNAPSHOT.jar
```

`DefaultAzureCredential` will automatically use your Azure CLI credentials.

### Kubernetes with Workload Identity

1. **Enable Workload Identity on your AKS cluster:**

```bash
az aks update \
  --name <aks-cluster-name> \
  --resource-group <resource-group> \
  --enable-oidc-issuer \
  --enable-workload-identity
```

2. **Create a Kubernetes Service Account:**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kafka-consumer-sa
  namespace: your-namespace
  annotations:
    azure.workload.identity/client-id: "<your-app-registration-client-id>"
```

3. **Deploy your application with the service account:**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-consumer
  namespace: your-namespace
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-consumer
  template:
    metadata:
      labels:
        app: kafka-consumer
        azure.workload.identity/use: "true"
    spec:
      serviceAccountName: kafka-consumer-sa
      containers:
        - name: kafka-consumer
          image: your-registry/kafka-consumer:latest
          env:
            - name: AZURE_CLIENT_ID
              value: "<your-app-registration-client-id>"
            - name: AZURE_TENANT_ID
              value: "<your-tenant-id>"
```

The workload identity webhook automatically injects:
- `AZURE_FEDERATED_TOKEN_FILE` - path to the projected service account token
- Mounts the token at the specified path

### Azure Container Apps / App Service

For managed Azure services, use Managed Identity:

1. Enable System-assigned or User-assigned Managed Identity on your resource
2. Create a federated credential linking the Managed Identity to your App Registration (if using User-assigned)
3. Set the `AZURE_CLIENT_ID` environment variable to your App Registration's Client ID

## Authentication Flow

```
+-------------------+     +-------------------+     +-------------------+
|   Application     |---->|   Entra ID        |---->|   Kafka Broker    |
|                   |     |                   |     |                   |
| 1. Request token  |     | 2. Validate       |     | 4. Validate       |
|    with OIDC      |     |    federated      |     |    OAuth token    |
|    assertion      |     |    credential     |     |                   |
|                   |<----|                   |     |                   |
|                   |     | 3. Return OAuth   |     |                   |
|                   |     |    token          |     |                   |
+-------------------+     +-------------------+     +-------------------+
```

## Troubleshooting

### "AADSTS70021: No matching federated identity record found"

- Verify the issuer URL matches exactly
- Check the subject claim matches your service account (`system:serviceaccount:<namespace>:<service-account-name>`)
- Ensure the federated credential is configured for the correct namespace and service account

### "AADSTS700024: Client assertion is not within its valid time range"

- Check that your cluster's time is synchronized
- The token may have expired - ensure token refresh is working

### Token acquisition fails locally

- Run `az login` to authenticate
- Verify you have access to the correct tenant: `az account show`
- Try `az account set --subscription <subscription-id>` if you have multiple subscriptions

## Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `AZURE_CLIENT_ID` | App Registration Client ID | Yes (in Kubernetes) |
| `AZURE_TENANT_ID` | Azure Tenant ID | Yes (in Kubernetes) |
| `AZURE_FEDERATED_TOKEN_FILE` | Path to OIDC token (auto-injected by AKS) | Auto |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker address | No (uses application.properties) |
| `KAFKA_GROUP_ID` | Consumer group ID | No (uses application.properties) |
| `KAFKA_TOPIC` | Kafka topic to consume | No (uses application.properties) |
| `KAFKA_AUTO_OFFSET_RESET` | Offset reset policy | No (uses application.properties) |
| `KAFKA_ENABLE_AUTO_COMMIT` | Auto commit offsets | No (uses application.properties) |
| `KAFKA_SECURITY_PROTOCOL` | Security protocol | No (uses application.properties) |
| `KAFKA_SASL_MECHANISM` | SASL mechanism | No (uses application.properties) |
| `KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | SSL endpoint identification | No (uses application.properties) |
| `KAFKA_SSL_CA_LOCATION` | Path to CA certificate | No (uses application.properties) |
| `KAFKA_ENABLE_INSECURE_SSL` | Disable SSL verification | No (uses application.properties) |
