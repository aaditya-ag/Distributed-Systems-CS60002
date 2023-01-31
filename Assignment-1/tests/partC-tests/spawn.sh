#!/bin/bash
broker=http://127.0.0.1:5000
for i in 1 2 3 4 5
do
    python producer.py $broker ../test_asgn1/producer_$i.txt &
done
python consumer.py $broker consumer_1 T-1 T-2 T-3 &
python consumer.py $broker consumer_2 T-1 T-3 &
python consumer.py $broker consumer_3 T-1 T-3 &
