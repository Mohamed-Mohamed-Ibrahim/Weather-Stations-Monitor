
# 📝 Kafka + Docker Commands Reference

## Notes
- ✅ Make sure **Docker Desktop is running** before executing any commands.
- 🧼 For Kafka commands: **remove** all `bin/` prefixes and `.sh` suffixes.

---

## 🐳 Docker Commands

### 🔹 Basic Docker Setup
```bash
sudo systemctl start docker                 # Start Docker service
sudo systemctl enable docker                # Enable Docker on startup
docker --version                            # Check Docker version
````

### 🔹 Docker Compose & Container Management

```bash
docker-compose up -d                        # Start services in detached mode
docker ps                                   # List running containers
docker exec -it project-kafka-1 bash        # Enter Kafka container shell
```

---

## 🧵 Kafka Commands

### 🔹 Topic Management

```bash
# Create a topic
kafka-topics --create --topic test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List all topics
kafka-topics --list --bootstrap-server localhost:9092

# Delete specific topics
kafka-topics --bootstrap-server localhost:9092 --delete --topic test
kafka-topics --bootstrap-server localhost:9092 --delete --topic weather-rain
kafka-topics --bootstrap-server localhost:9092 --delete --topic weather-archive
kafka-topics --bootstrap-server localhost:9092 --delete --topic weather-raw
kafka-topics --bootstrap-server localhost:9092 --delete --topic weather-dropped
kafka-topics --bootstrap-server localhost:9092 --delete --topic my_first
kafka-topics --bootstrap-server localhost:9092 --delete --topic falafil
```

### 🔹 Producing Messages

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic test
```

### 🔹 Consuming Messages

```bash
# Consume all messages from the beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning

# Consume only the first 2 messages from the beginning
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning --max-messages 2
```

---



