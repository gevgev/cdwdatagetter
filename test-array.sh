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

file="event/tv_viewership/4000002/delta/20160630-062701000_20160630/tv_viewership.cod.bz2"

file2="${file/.bz2/}"

echo $file
echo $file2