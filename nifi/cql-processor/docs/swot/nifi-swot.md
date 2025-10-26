# NiFi SWOT analysis

The SWOT analysis from perspective of Strengths, Weaknesses, Opportunities
and Threats.


## 1. Strengths
  Strengths: items as advantages, benefits.

  ### 1.1 Pricing:
  - free (open source)

  ### 1.2 Functionalities
  - Support on-prem, cloud
  - Prevention of system overloading (via throttling)
  - Throttling as default behavior (via amount of items in queue and 
    total queue size)
  - Native formats CSV, XML, JSON

  ### 1.3 Connectors
  - File transfer, AWS S2, Azure BlobStorage
  - SQL (Postgres, MySQL)
  - NoSQL (Mongo)
  - Event streaming (MQ, Kafka)

  ### 1.4 Environment
  - Java 21
  - GitHub, GitLab

  ### 1.5 Key contributors
  - Cloudera
  - Snowflake
  - Ksolves
  - etc. (see https://nifi.apache.org/community/powered-by/)

## 2. Weaknesses
  Weaknesses: items as disadvantages, limited functions.

  - without deployment functionalities (package & env. extensions)
  - only external relation on CI/CD
  - limited privileges (without setting in project level)

## 3. Opportunities

  Opportunities: items could exploit to its advantage, future direction.

## 4. Threats

  Threats: items that could cause trouble, risk.