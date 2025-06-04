# CloudEvents + Kafka (Redpanda) Demo

This is a simple Java-based demo that shows how to produce and consume [CloudEvents](https://cloudevents.io/) using Apache Kafka (via [Redpanda](https://redpanda.com/)).

---

## 🔧 Requirements

- Java 17+ (`java -version`)
- Maven (`mvn -v`)
- Docker + Docker Compose
- Git (optional)

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/cloudevents-kafka-demo.git <!-- Replace your-org and upload project to github -->
cd cloudevents-kafka-demo
```

---

### 2. Start Kafka (Redpanda) + Kafka UI

Make sure Docker is running, then run:

```bash
docker compose up -d
```

This will start:

- Redpanda (Kafka-compatible broker)
- Kafka UI (http://localhost:8080)

📝 You can monitor messages using Kafka UI (optional), or `rpk` in the terminal.

---

### 3. Build the Project

```bash
mvn compile
```

---

### 4. Run the Kafka Producer

This will produce a CloudEvent to the Kafka topic `demo-topic`.

```bash
mvn exec:java -Dexec.mainClass="com.example.App"
```

---

### 5. Run the Kafka Consumer

In a **new terminal window**:

```bash
mvn exec:java -Dexec.mainClass="com.example.CloudEventConsumer"
```

You should see the CloudEvent printed to the console.

---

### 6. Optional: View the Message Using `rpk`

You can also inspect messages using Redpanda’s CLI:

```bash
docker exec -it redpanda rpk topic consume demo-topic
```

(Press `Ctrl + C` to exit)

---

## 🧱 Project Structure

```
cloudevents-kafka-demo/
├── docker-compose.yml
├── pom.xml
├── README.md
└── src/
    └── main/
        └── java/
            └── com/
                └── example/
                    ├── App.java
                    └── CloudEventConsumer.java
```

---

## 📦 Dependencies

- `org.apache.kafka:kafka-clients`
- `io.cloudevents:cloudevents-core`
- `io.cloudevents:cloudevents-kafka`
- `io.cloudevents:cloudevents-json-jackson`
- `com.fasterxml.jackson.core:jackson-databind`

---

## 📚 References

- [CloudEvents SDK for Java](https://github.com/cloudevents/sdk-java)
- [Redpanda Docs](https://docs.redpanda.com/)
- [Kafka UI](https://github.com/provectus/kafka-ui)
