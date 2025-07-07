# 🔍 Spark Log Analyzer

A modular PySpark-based system for detecting security threats from raw web server logs. 

## 🚀 Features

- 📁 **Log Ingestion**: Supports reading raw Apache access logs from local files
- 🧩 **Log Parsing**: Uses regex-based parsing and timestamp normalization
- 🧠 **Attack Detection**:
  - Brute-force attack detection using login URL frequency + time bucket analysis
  - DDoS detection via IP-wise sliding time window aggregation
  
- ⚙️ **Modular Components**:
  - Configurable thresholds and detection logic
  - Class-based architecture: `Config`, `Reader`, `Parser`, `Analyzer`

