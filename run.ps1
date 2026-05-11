param(
    [string]$class = "projet.LogParser"
)

$JAR = "C:\Users\Lenovo\bigdata-cluster\spark-streaming\target\scala-2.13\log-streaming-assembly-1.0.jar"

Write-Host "Copie du JAR..."
docker cp $JAR spark-master:/opt/spark/jars/log-streaming.jar
docker cp $JAR spark-worker-1:/opt/spark/jars/log-streaming.jar
docker cp $JAR spark-worker-2:/opt/spark/jars/log-streaming.jar

Write-Host "Lancement de $class..."
docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --driver-memory 512m `
  --executor-memory 512m `
  --total-executor-cores 2 `
  --conf spark.jars.ivy=/tmp/ivy2 `
  --conf spark.executor.extraClassPath=/opt/spark/jars/log-streaming.jar `
  --class $class `
  /opt/spark/jars/log-streaming.jar



