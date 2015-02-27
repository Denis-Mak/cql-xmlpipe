@echo off
if defined JAVA_HOME (set JAVA_CMD="%JAVA_HOME%/bin/java") ELSE (set JAVA_CMD=java)
%JAVA_CMD% -Dfile.encoding=UTF-8 -cp !!artifactId!!-!!version!!.jar ru.factsearch.Query %*