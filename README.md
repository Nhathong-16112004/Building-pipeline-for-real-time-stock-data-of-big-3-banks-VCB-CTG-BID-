# Building Pipeline for Real-Time Stock Data of Big 3 Banks
The project focuses on collecting and processing real-time stock price data from Yahoo Finance for 3 major banks in Vietnam:
Vietcombank (VCB.VN)
VietinBank (CTG.VN)
BIDV (BID.VN)
# Objectives include:
Building a real-time streaming system from input (producer) to storage (PostgreSQL), analyzing price indices in a 10-minute time window and visualizing data using Superset.
# Implementation process:
1. Data collection: Use Python Producer to crawl data from Yahoo Finance API. Data is sent in real time every 60 seconds to Kafka Topic
2. Real-time Processing: Use Apache Kafka to make message queue. Data is read continuously using Apache Spark Streaming.
3. Data storage: Processed data is written to PostgreSQL.
4. Visualization: Use Apache Superset to connect directly to PostgreSQL. Build a Dashboard to support real-time monitoring of stock price movements.
# Achievements
Successfully built a real-time pipeline:

Yahoo Finance → Kafka → Spark → PostgreSQL → Superset

Designed an automated system that can run 24/7.

Superset Dashboard displays real-time price data.
