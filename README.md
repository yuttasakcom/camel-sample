# camel-sample

ตัวอย่างระบบส่งต่อ transaction ของธนาคารผ่าน Kafka โดยใช้ Apache Camel ย้ายข้อความระหว่าง topic

- **Go producer** จำลอง transaction ของธนาคาร แล้วส่งเข้า topic `transaction-logs`
- **Camel router** อ่านจาก `transaction-logs` แล้วส่งต่อไป topic `consumer-ddp`
- **Java consumer** (Camel) อ่านจาก `consumer-ddp` แล้ว print log ของแต่ละ transaction

## แผนภาพการทำงาน

```mermaid
flowchart LR
    subgraph Docker["Docker Compose"]
        direction TB
        subgraph Kafka["Kafka (KRaft) :9092"]
            T1[("transaction-logs")]
            T2[("consumer-ddp")]
        end
        UI["Kafka UI<br/>:8080"]
    end

    GO["Go producer<br/>go/cmd/main.go"] -- "JSON transaction<br/>key = เลขบัญชี" --> T1
    T1 -- "group: camel-transaction-router" --> CAMEL["Camel router<br/>camel/"]
    CAMEL -- "ส่งต่อ body + key เดิม<br/>acks=all" --> T2
    T2 -- "group: consumer-ddp-group" --> JAVA["Java consumer<br/>java/"]
    JAVA --> LOG["Console log"]
    UI -. "ดู topic / message" .-> Kafka
```

ลำดับการทำงานของ 1 transaction:

```mermaid
sequenceDiagram
    participant G as Go producer
    participant K1 as transaction-logs
    participant C as Camel router
    participant K2 as consumer-ddp
    participant J as Java consumer

    loop ทุก PRODUCE_INTERVAL (ค่าเริ่มต้น 1s)
        G->>G: สุ่ม transaction
        G->>K1: ส่ง JSON (key = เลขบัญชี)
    end
    C->>K1: poll
    K1-->>C: message
    C->>K2: ส่งต่อแบบไม่แก้เนื้อหา (key เดิม)
    J->>K2: poll
    K2-->>J: message
    J->>J: แปลง JSON เป็น Transaction
    alt JSON ถูกต้อง
        J->>J: log สรุป transaction (INFO)
    else JSON ไม่ถูกต้อง
        J->>J: log ข้อความต้นฉบับ (ERROR) แล้วอ่านข้อความถัดไป
    end
```

## โครงสร้างโปรเจกต์

```
.
├── docker-compose.yml      # Kafka (KRaft) + Kafka UI
├── clear-message.sh        # ลบ message ใน topic
├── go/                     # Producer
│   └── cmd/main.go
├── camel/                  # Router: transaction-logs -> consumer-ddp
│   └── src/main/java/com/yuttasak/camel/
│       ├── Application.java
│       └── TransactionRouterRoute.java
└── java/                   # Consumer: consumer-ddp -> log
    └── src/main/java/com/yuttasak/ddp/
        ├── Application.java
        ├── Transaction.java
        └── TransactionConsumerRoute.java
```

## รายละเอียดแต่ละส่วน

### 1. Kafka + Kafka UI (`docker-compose.yml`)

| Service | Image | Port | หมายเหตุ |
|---|---|---|---|
| kafka | `apache/kafka:4.0.0` | 9092 | โหมด KRaft ไม่ต้องใช้ Zookeeper |
| kafka-ui | `kafbat/kafka-ui` | 8080 | เริ่มทำงานหลังจาก Kafka ผ่าน healthcheck |

- โปรแกรมที่รันบนเครื่อง (host) ต่อที่ `localhost:9092`
- container อื่นใน compose เดียวกันต่อที่ `kafka:29092`
- ข้อมูลเก็บใน volume `kafka-data`
- ถ้ายังไม่มี topic จะถูกสร้างให้อัตโนมัติ

### 2. Go producer (`go/`)

สุ่ม transaction แล้วส่งเข้า `transaction-logs` ตามรอบเวลาที่ตั้งไว้ ใช้ library `segmentio/kafka-go`

ตัวอย่าง message:

```json
{
  "transactionId": "b09d51a6-dc68-4c2d-8305-1edc9daa5dc9",
  "type": "WITHDRAW",
  "fromAccount": "678-9-01234-5",
  "amount": 35236.46,
  "currency": "THB",
  "channel": "ATM",
  "status": "SUCCESS",
  "timestamp": "2026-10-03T09:42:07.050483Z"
}
```

| Field | ค่าที่เป็นไปได้ |
|---|---|
| `type` | `DEPOSIT` (มีแค่ `toAccount`), `WITHDRAW` / `PAYMENT` (มีแค่ `fromAccount`), `TRANSFER` (มีทั้งสองบัญชี และเป็นคนละบัญชีกัน) |
| `channel` | `MOBILE`, `ATM`, `BRANCH`, `INTERNET` |
| `status` | `SUCCESS` (ออกบ่อยที่สุด), `FAILED`, `PENDING` |
| `amount` | 1.00 – 50,000.99 THB |

- ใช้เลขบัญชีเป็น key ของ message ข้อความของบัญชีเดียวกันจึงเข้า partition เดียวกันและเรียงลำดับถูกต้อง
- รอให้ Kafka ยืนยันการรับครบก่อน (`RequireAll`)
- กด Ctrl+C แล้วโปรแกรมจะปิดการเชื่อมต่อให้เรียบร้อยก่อนจบ

### 3. Camel router (`camel/`)

ใช้ Apache Camel 4.18.4 route `transaction-logs-to-consumer-ddp` ทำงานดังนี้

1. อ่านข้อความจาก `transaction-logs`
2. log ตำแหน่ง partition, offset และ key
3. ส่งต่อไป `consumer-ddp` แบบไม่แก้เนื้อหาและใช้ key เดิม โดยรอให้ Kafka ยืนยันการรับครบ (`requestRequiredAcks=all`)

ถ้าส่งไม่สำเร็จจะลองใหม่ 3 ครั้ง ห่างกันครั้งละ 1 วินาที

### 4. Java consumer (`java/`)

ใช้ Apache Camel 4.18.4 route `consume-ddp-transactions` ทำงานดังนี้

1. อ่านข้อความจาก `consumer-ddp`
2. แปลง JSON เป็น record `Transaction` ด้วย Jackson ถ้ามี field อื่นที่ไม่รู้จักจะข้ามไป
3. log สรุป transaction 1 บรรทัด

```
partition=0 offset=45 key=678-9-01234-5 | dc20bb6b-... DEPOSIT     34,393.39 THB | from=- to=678-9-01234-5 | ATM      SUCCESS | 2026-10-03T09:42:07Z
```

ถ้าข้อความไม่ใช่ JSON ที่ถูกต้อง จะ log เป็น ERROR พร้อมข้อความต้นฉบับ แล้วอ่านข้อความถัดไปต่อ ไม่หยุดทำงาน

## วิธีรัน

### สิ่งที่ต้องมี

- Docker และ Docker Compose
- Go 1.24 ขึ้นไป
- Java 21 ขึ้นไป (ไม่ต้องติดตั้ง Maven เพราะในโปรเจกต์มี `./mvnw` ให้แล้ว)

### ขั้นตอน

**1. เปิด Kafka และ Kafka UI**

```bash
docker compose up -d
```

**2. เปิด Camel router** (terminal 1)

```bash
cd camel
./mvnw compile exec:java
```

**3. เปิด Java consumer** (terminal 2)

```bash
cd java
./mvnw compile exec:java
```

**4. เปิด Go producer** (terminal 3)

```bash
cd go
go run ./cmd
```

หลังจากนั้น terminal 1 จะเห็น log `move ... -> consumer-ddp` และ terminal 2 จะเห็น log สรุป transaction

**5. ดูข้อความผ่าน Kafka UI**

เปิด http://localhost:8080 → Topics → `transaction-logs` หรือ `consumer-ddp`

**6. ลบ message ใน topic** (ถ้าต้องการเริ่มทดสอบใหม่)

```bash
./clear-message.sh                  # ลบใน transaction-logs และ consumer-ddp
./clear-message.sh transaction-logs # ลบเฉพาะ topic ที่ระบุ
```

ลบเฉพาะ message ส่วนตัว topic และการตั้งค่ายังอยู่ message ใหม่จะได้ offset ต่อจากเดิม

**7. ปิดระบบ**

กด Ctrl+C ในแต่ละ terminal แล้วรัน

```bash
docker compose down      # ปิด Kafka
docker compose down -v   # ปิด Kafka และลบข้อมูลทั้งหมด
```

## การตั้งค่าผ่าน environment variable

| ส่วน | ตัวแปร | ค่าเริ่มต้น |
|---|---|---|
| Go producer | `KAFKA_BROKER` | `localhost:9092` |
| | `PRODUCE_INTERVAL` | `1s` (รูปแบบ Go duration เช่น `200ms`, `5s`) |
| Camel router | `KAFKA_BROKERS` | `localhost:9092` |
| | `KAFKA_SOURCE_TOPIC` | `transaction-logs` |
| | `KAFKA_TARGET_TOPIC` | `consumer-ddp` |
| | `KAFKA_GROUP_ID` | `camel-transaction-router` |
| Java consumer | `KAFKA_BROKERS` | `localhost:9092` |
| | `KAFKA_TOPIC` | `consumer-ddp` |
| | `KAFKA_GROUP_ID` | `consumer-ddp-group` |

topic `transaction-logs` ของ Go producer กำหนดไว้ในโค้ด (`const topic`) เปลี่ยนผ่าน env ไม่ได้

ตัวอย่าง:

```bash
PRODUCE_INTERVAL=200ms go run ./cmd
KAFKA_TOPIC=transaction-logs ./mvnw compile exec:java   # ให้ Java อ่านจาก Go ตรงๆ โดยไม่ผ่าน Camel
```

## ข้อควรรู้

- **อ่านจากข้อความแรกสุด:** ทั้ง Camel และ Java ตั้ง `auto-offset-reset = earliest` ตอนรันครั้งแรก (หรือเปลี่ยน group id) จึงอ่านข้อความเก่าทั้งหมดใน topic ถ้าต้องการอ่านเฉพาะข้อความใหม่ ให้เปลี่ยนเป็น `latest` ใน `application.properties`
- **ข้อความอาจซ้ำได้:** Camel router ไม่ได้ป้องกันข้อความซ้ำ ถ้าโปรแกรมหยุดกลางทาง ข้อความบางรายการอาจถูกส่งไป `consumer-ddp` ซ้ำ ถ้าห้ามซ้ำเด็ดขาด ต้องเปลี่ยนให้ยืนยันการอ่านด้วยตัวเอง (manual commit) หรือใช้ Kafka transaction
- **รันใน container:** ถ้าจะรันโปรแกรมเหล่านี้เป็น container ใน compose เดียวกัน ให้ตั้ง broker เป็น `kafka:29092`
