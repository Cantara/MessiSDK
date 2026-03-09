# MessiSDK

## Purpose
Core SDK for the Messi messaging and streaming abstraction layer. Provides a unified API for producing and consuming messages across different messaging backends (Kinesis, SQS, S3) without coupling application code to specific cloud services.

## Tech Stack
- Language: Java 11+
- Framework: None (pure library)
- Build: Maven
- Key dependencies: Protocol Buffers, SLF4J

## Architecture
Abstract messaging SDK using the Java SPI provider pattern. Defines interfaces for message production, consumption, and stream management. Concrete implementations (MessiKinesisProvider, MessiSQSProvider, MessiS3Provider) are loaded at runtime via ServiceLoader. Messages use Protocol Buffers for efficient serialization.

## Key Entry Points
- Core Messi interfaces and API classes
- `pom.xml` - Maven coordinates: `no.cantara.messi:messi-sdk`

## Development
```bash
# Build
mvn clean install

# Test
mvn test
```

## Domain Context
Cloud-agnostic messaging abstraction. The core SDK that enables portable message-driven architectures, allowing applications to switch between AWS Kinesis, SQS, and S3 backends without code changes. Foundation of the Messi ecosystem.
