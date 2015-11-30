#!/bin/bash

tail -n+2 left.csv | sort -t , -k 1,1 > sortedl.txt
tail -n+2 right.csv | sort -t , -k 3,3 > sortedr.txt
join -t , -1 1 -2 3 sortedl.txt sortedr.txt > join.csv

rm sortedl.txt
rm sortedr.txt
