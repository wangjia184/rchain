#!/bin/zsh

_JAVA_OPTIONS="-Xms2G -Xmx4G -Xss2m -XX:MaxMetaspaceSize=1G -Dsbt.task.timings=true -Dsbt.task.timings.on.shutdown=true -Dsbt.task.timings.threshold=2000" sbt clean bnfc:clean bnfc:generate compile node/docker:publishLocal
