#!/usr/bin/env bash

spark-submit \
    --master local \
    --py-files etl/extract.py,etl/mapping.py \
    lace_score.py $1