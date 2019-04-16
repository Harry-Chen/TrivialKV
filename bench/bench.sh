#!/bin/bash
for skew in 0 1
do
	for ratio in {0..100..5}
	do
		for thread in {1..64..1}
		do
			echo "./bench $thread $ratio $skew | tee ${thread}_${ratio}_${skew}.log"
			echo "rm -rf data"
			echo "sleep 5"
		done
	done
done
