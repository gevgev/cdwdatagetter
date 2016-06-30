#!/bin/sh
#set -x

N=0
ARR=()

IFS=","

while read STR
do
        set -- "$STR"

        while [ "$#" -gt 0 ]
        do
                ARR[$N]="$1"
                ((N++))
                shift
        done
done < mso-list-full.csv

for provider in "${ARR[@]}"
	do 
		echo "$provider"
done