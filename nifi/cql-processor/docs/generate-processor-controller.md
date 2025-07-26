
# Generate NiFi processor and controller

## 1. Install maven
   1. [Install maven sources](https://maven.apache.org/install.html)
   2. Install JDK (MUST)
      - Setup variable **JAVA_HOME**=c:\Program Files\Java\jdk-21\
   3. [Download maven](https://maven.apache.org/download.cgi)
   4. Update variable **PATH**=...;c:\Apps\maven\bin;

## 2. Generate skellet
   1. Command line: **mvn archetype:generate**
   2. Filter: **nifi**
   3. Dialog settings, e.g.:
      1. groupId:          org.george0st
      2. artifactId:       sample-processor or sample-controller
      3. version:          1.0
      4. artifactBaseName: sample