param(
    [string]$class = "projet.LogParser"
)

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