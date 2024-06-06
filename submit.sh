spark-submit \
    --master yarn \
    --deploy-mode cluster workload.py \
    --output $2 \
    --input $1
