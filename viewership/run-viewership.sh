#!/bin/bash
set -x

if [ "$#" -ne 10 -a "$#" -ne 11 ]; then
    echo "Error: Missing parameters:"
    echo "  AWS_access_key"
    echo "  AWS_access_secret"
    echo "  s3_bucket"
    echo "  data_downloader_activity_tracker_file"
    echo "  data_downloader_status_log_dir"
    echo "  data_download_destination"
    echo "  output_files_dir"
    echo "  diamonds_delimited_filename"
    echo "  base_folder for cdw-s3-structure/rovi-cdw/event/tv_viewership/<provider>/delta"
    echo "  mso list"
    echo "  date (optional). Format:yyyyMMdd, example: 20160630"
    exit 1
fi


access_key=$1 
access_secret=$2 

bucket=$3 
#/rovi-cdw

# tracker log to keep info of previously processed files
data_downloader_activity_tracker_file=$4 
# "data_downloader_tracker.txt"

# directory to log the activity of the script.
data_downloader_status_log_dir=$5 
# "cdw_downloads_logs"
echo `mkdir "$data_downloader_status_log_dir"`

# directory where the cdw data compressed files will be written
data_download_destination=$6
# "input_compressed_cdw_data"
echo `mkdir "$data_download_destination"`

# directory where the final counting reports will be written
output_files_dir=$7
#"cdw-data-reports"
echo `mkdir "$output_files_dir"`

# name of file after changing the control A to diamonds
diamonds_delimited_filename=$8
#"tv_viewership.cod"

# base folder for cdw-s3-structure/rovi-cdw/event/tv_viewership/$provider/delta
base_folder=$9
# "event/tv_viewership"

mso_list=${10}


#date the script run
if [ "$#" == 11 ]; then
    as_of=${11}
else
    as_of=`date +"%Y%m%d"`
fi 

#sp providers are listed as codes: 
#8000200  (blueridge palmerton)
#8000150  (panhandle guymon)
#4000200  (armstrong bulter)
#4000050  (midcontinent )
#4000013  (mediacom albany)
#4000012  (mediacom moline)
#4000011  ( mediacom Demoines)
#4000002 (htc)
#  It will be ultimate to map code to provider name
# may be to do in the future.

# 4000002, HTC
# 4000011, Mediacom-Des Moines
# 4000012, Mediacom-Moline
# 4000013, Mediacom-Albany
# 4000200, Armstrong-Butler
# 8000150, Panhandle-Guymon
# 8000200, Blueridge-Palmerton
# 4000050, MidCo

N=0
arr=()

IFS=","

while read STR
do
        set -- "$STR"

        while [ "$#" -gt 0 ]
        do
                arr[$N]="${1%,*}"
                ((N++))
                shift
        done
done < "$mso_list"


# Run the aws s3 data getter 
AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$access_secret" ./cdwdatagetter -r us-east-1 -b "$bucket" -d "$as_of" -p "$base_folder" -m "$mso_list"

for provider in "${arr[@]}"
    do
        # get the latest file in the latest subdirectory for that provider
    FILES="$base_folder/$provider/delta/*/*"
    # get the latest file in the latest subdirectory for that provider
    for file in $FILES
        do  

            # uncompress the the tv_viewership.cod.bz2 file

            gunzip -f ${file}
            # replace all Control A with Diamnonds
            cat -v "${file/.bz2/}" | LANG=C sed 's/\^A/<>/g' > $data_download_destination/$diamonds_delimited_filename

            # create the subdirectory structure for today run and make it writeable
            mkdir $output_files_dir/$as_of;
            chmod a+rw $output_files_dir/$as_of;

            # create the subdirectory structure for provider and make it writeable
            mkdir $output_files_dir/$as_of/$provider;
            chmod a+rw $output_files_dir/$as_of/$provider;

            # create the csv report file for a given provider
            echo " creating csv file $output_files_dir/$as_of/$provider/tv_viewership-$provider-$as_of.csv"
            touch $output_files_dir/$as_of/$provider/tv_viewership-$provider-$as_of.csv

            # create the headings in the csv report file
            echo "hh_id, ts, pg_id, pg_name, ch_num, ch_name, event, zipcode, country" > $output_files_dir/$as_of/$provider/tv_viewership-$provider-$as_of.csv

            # convert the diamonds into pipes and filter out the lines with soft power off. only the "channel tune" events will be in the output.
            # awk q=\' 'BEGIN{FS="<>"; OFS=","} $13 == "channel tune" { print $25, $39, $14, $15, $93, $63, $13, $35, $34}' $data_download_destination/$diamonds_delimited_filename >> $output_files_dir/$as_of/$provider/tv_viewership-$provider-$as_of.csv
            awk -v 'q='\''' 'BEGIN{FS="<>"; OFS=","} $13 == "channel tune" { print $25, $14, $91, "\""$93"\"", $70, $63, $13, $35, $34}' $data_download_destination/$diamonds_delimited_filename >> $output_files_dir/$as_of/$provider/tv_viewership-$provider-$as_of.csv
            # change the channel tune value to watch. (preserve the original in *.bak)
            LANG=C sed -i .bak 's/channel tune/watch/g' $output_files_dir/$as_of/$provider/tv_viewership-$provider-$as_of.csv

            echo " deleting processed file $data_download_destination/$diamonds_delimited_filename after getting viewership reports for $provider on $as_of "
            rm  $data_download_destination/$diamonds_delimited_filename

        done
    echo " cdw data downloader has finished processing the newest file ${file} for $provider  "
    echo " cdw data downloader has finished processing newest file: ${file}  for $provider" >> $data_downloader_status_log_dir/cdw-data-downloader.log

done

echo " cdw data downloader has finished downloading files. "
echo " cdw data downloader has finished downloading files. " >> $data_downloader_status_log_dir/cdw-data-downloader.log

# aws-s3-uploader will use the EC2 role to access daap-hh-count s3 bucket
#echo " Pushing to AWS S3"
#./aws-s3-uploader -p "$output_files_dir" -b daap-hh-count -z=true

#echo " Clean everything"
#rm -fr "$output_files_dir"
#rm -fr "$data_downloader_status_log_dir"
#rm -fr "$base_folder"
#rm -fr "$data_download_destination"

