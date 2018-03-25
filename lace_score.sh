#!/usr/bin/env bash

spark-submit \
    --master local \
    lace_score.py $1