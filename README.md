# Redis Implementation in Java: Project Summary

## Overview

This project is a custom implementation of a Redis server in Java, designed to replicate core Redis functionality, including key-value storage, streams, lists, and replication. The codebase is modular, with distinct classes handling different aspects of the server, making it maintainable and extensible. It supports Redis commands like `SET`, `GET`, `XADD`, `XRANGE`, `XREAD`, `RPUSH`, `LPUSH`, `LPOP`, `BLPOP`, `WAIT`, and replication features such as master-replica synchronization.

The project was developed as part of a challenge (e.g., CodeCrafters Redis challenge) and has been tested up to Stage #NA2 (Replication - WAIT with multiple commands). This document summarizes the codebase structure, a critical issue encountered with the `WAIT` command in replication, the fix applied, and instructions for running and extending the project.

## Project Structure

The implementation is organized into several Java classes, each with a specific responsibility:

- **Main.java**: The entry point of the application. It parses command-line arguments (e.g., `--port`, `--replicaof`, `--dir`, `--dbfilename`), loads RDB files for persistence, starts the server socket, and spawns threads for client and replica connections.
- **KeyValueStore.java**: Manages key-value pair storage with support for expiration (via `PX` option in `SET`) and commands like `GET`, `INCR`, and `KEYS`.
- **StreamStore.java**: Handles Redis stream operations, including `XADD`, `XRANGE`, and `XREAD`, with support for blocking reads.
- **ListStore.java**: Implements Redis list operations like `RPUSH`, `LPUSH`, `LRANGE`, `LPOP`, and `BLPOP`.
- **ClientHandler.java**: Processes incoming client commands, parses RESP protocol messages, and delegates to appropriate stores (`KeyValueStore`, `StreamStore`, `ListStore`) or the `ReplicationManager`.
- **RESPParser.java**: Parses and builds RESP (Redis Serialization Protocol) messages, handling arrays, bulk strings, and simple strings.
- **ReplicationManager.java**: Manages replication by propagating commands to replicas, handling `PSYNC`, and coordinating `WAIT` command acknowledgments.
- **ReplicaConnection.java**: Represents a connection to a replica, storing the socket, input/output streams, and replication offset.
- **ReplicaClient.java**: Implements the replica’s logic to connect to a master, perform the handshake (`PING`, `REPLCONF`, `PSYNC`), and process propagated commands.

The codebase uses `ConcurrentHashMap` for thread-safe storage and supports multi-threading to handle multiple clients and replicas concurrently.

## Key Features

- **RESP Protocol**: Fully implements the Redis Serialization Protocol for client-server communication.
- **Persistence**: Loads key-value pairs from RDB files specified via `--dir` and `--dbfilename`.
- **Replication**: Supports master-replica replication with `PSYNC`, command propagation, and the `WAIT` command for synchronization.
- **Data Structures**: Implements key-value pairs, streams, and lists with commands like `SET`, `GET`, `INCR`, `XADD`, `XRANGE`, `XREAD`, `RPUSH`, `LPUSH`, `LPOP`, and `BLPOP`.
- **Transactions**: Supports `MULTI`, `EXEC`, and `DISCARD` for atomic command execution.
- **Configuration**: Parses command-line arguments for port, replica settings, and RDB file paths.

 **Impact**: This ensures replicas only receive propagated commands or `REPLCONF GETACK *`, aligning with the Redis protocol and fixing the test failure.

## Build Warnings
During compilation, two warnings were noted:
1. **Jansi Warning**: `java.lang.System::load` was called by `jansi-2.4.1.jar`. This can be suppressed by adding `--enable-native-access=ALL-UNNAMED` to the Maven compiler plugin in `pom.xml`:
   ```xml
   <plugin>
       <groupId>org.apache.maven.plugins</groupId>
       <artifactId>maven-compiler-plugin</artifactId>
       <version>3.13.0</version>
       <configuration>
           <source>17</source>
           <target>17</target>
           <compilerArgs>
               <arg>--enable-native-access=ALL-UNNAMED</arg>
           </compilerArgs>
       </configuration>
   </plugin>
   ```
2. **Guava Warning**: `sun.misc.Unsafe::objectFieldOffset` was called by `guava-33.2.1-jre.jar`. This is a deprecated method in a third-party library. For now, it can be ignored, but consider reporting it to the Guava maintainers or upgrading to a newer Guava version.

## How to Run the Project

### Prerequisites
- Java 17 or later
- Maven (for dependency management and building)
- A Redis client (e.g., `redis-cli`) for testing

### Setup
1. **Clone the Code**: Ensure all Java files (`Main.java`, `KeyValueStore.java`, `StreamStore.java`, `ListStore.java`, `ClientHandler.java`, `RESPParser.java`, `ReplicationManager.java`, `ReplicaConnection.java`, `ReplicaClient.java`) are in the `src/main/java` directory.
2. **Configure `pom.xml`**: Ensure your `pom.xml` includes the necessary dependencies and the compiler configuration above.
3. **Compile**: Run `mvn clean compile` to build the project.
4. **Run the Server**:
   - As a master: `./your_program.sh --port 6379`
   - As a replica: `./your_program.sh --port 6380 --replicaof "localhost 6379"`
   - For persistence: `./your_program.sh --port 6379 --dir /path/to/dir --dbfilename dump.rdb`

### Testing
- Use `redis-cli` to connect to the server (e.g., `redis-cli -p 6379`).
- Test commands like:
  ```bash
  SET key value
  GET key
  XADD mystream * field1 value1
  XRANGE mystream - +
  RPUSH mylist item1 item2
  LPOP mylist
  WAIT 1 500
  ```
- For replication, start multiple instances as replicas and verify command propagation using `WAIT`.

## Extending the Project
To add new features or commands:
1. **Add to `ClientHandler`**: Implement a new case in the `handle` method’s switch statement and delegate to the appropriate store.
2. **Create New Stores**: For new data structures (e.g., sets or hashes), create a new class similar to `StreamStore` or `ListStore`.
3. **Enhance Replication**: Add support for partial resynchronization or more complex replication scenarios by modifying `ReplicationManager` and `ReplicaClient`.
4. **Improve Persistence**: Extend `Main.loadRDBFile` to support additional data types in RDB files.

## Known Issues and Future Improvements
- **Race Conditions in `waitForAcks`**: The `Thread.sleep(5)` in `ReplicationManager.waitForAcks` may cause delays or missed ACKs. Consider using a more robust synchronization mechanism (e.g., `ExecutorService` or `CompletableFuture`).
- **Connection Stability**: The `Connection reset` errors suggest socket handling could be improved. Add better error handling in `ReplicaConnection` and `ReplicaClient`.
- **Performance**: Optimize `StringBuilder` usage in `ClientHandler` for large responses and consider using a connection pool for replicas.

## Conclusion
This Redis implementation provides a solid foundation for a Redis-compatible server with support for key-value operations, streams, lists, and replication. The `WAIT` command issue was resolved by fixing the `REPLCONF ACK` response in `ClientHandler`, ensuring replicas receive only expected commands. The codebase is well-structured and ready for further extensions. For any issues or contributions, please test thoroughly with `redis-cli` and share detailed logs.

*Written by mysticjoel, July 29, 2025*