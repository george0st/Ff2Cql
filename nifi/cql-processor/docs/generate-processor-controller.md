
# Generate NiFi processor and controller

0. [Install maven](https://maven.apache.org/install.html)
   1. JDK is MUST
   2. Setup variable **JAVA_HOME**=c:\Program Files\Java\jdk-21\
   3. [Download maven](https://maven.apache.org/download.cgi)
   4. Update variable **PATH**=...;c:\Apps\maven\bin;
1. **mvn archetype:generate**
2. nifi
3. Dialog settings, e.g.:
   1. groupId:          org.george0st
   2. artifactId:       sample-processor or sample-controller
   3. version:          1.0
   4. artifactBaseName: sample