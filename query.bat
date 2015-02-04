@echo off
chcp 65001
if defined JAVA_HOME (set JAVA_CMD="%JAVA_HOME%/bin/java") ELSE (set JAVA_CMD=java)
%JAVA_CMD% -Dfile.encoding=UTF-8 -cp cql-xmlpipe-1.0-SNAPSHOT.jar ru.factsearch.Query %*