# Stock Signal Engine

A real-time stock trading signal generator built with Apache Flink, using
Complex Event Processing (CEP) to detect trading signals from a simulated
stream of stock price/volume ticks.

## What it should do after completion 🏗️
Ingests a stream of synthetic stock tick events (symbol, price, volume,
timestamp) and applies CEP pattern matching to detect signals such as:
- Moving average crossovers
- Price movement + volume spike correlation (within a time window)

## Tech stack
- Apache Flink 1.20.5 (DataStream API + FlinkCEP)
- Java 17
- Maven
