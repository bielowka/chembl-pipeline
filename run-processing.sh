#!/bin/bash

# Opcjonalnie: upewnij się, że najnowsza wersja skryptu jest w folderze współdzielonym
# cp pipeline.py data_platform/data_share/  <-- Odkomentuj, jeśli edytujesz plik w innym miejscu

echo "🚀 Uruchamianie zadania Spark w kontenerze Docker..."

#docker exec -i spark-master /opt/spark/bin/spark-submit \
#  --master spark://spark-master:7077 \
#  --packages org.postgresql:postgresql:42.6.0 \
#  --conf spark.jars.ivy=/tmp/.ivy \
#  --driver-memory 1G \
#  --executor-memory 1G \
#  /opt/spark/data/pipeline.py

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark/data/postgresql-42.6.0.jar \
  --driver-memory 1G \
  --executor-memory 1G \
  /opt/spark/data/pipeline.py \
  --mode HUMAN \
  --output_path /opt/spark/data/chembl_egfr_human.parquet


echo "✅ Zadanie zakończone!"