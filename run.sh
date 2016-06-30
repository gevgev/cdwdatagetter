#!/bin/sh
set -x

if [ "$#" -ne 9 -a "$#" -ne 10 ]; then
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
    exit 1
fi


access_key=$1 
access_secret=$2 

bucket=$3 
#/rovi-cdw

#date the script run
as_of=`date +"%Y%m%d"`

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

if [ ! -f  $data_downloader_activity_tracker_file ]
then
   touch $data_downloader_activity_tracker_file;
fi

# 4000002, HTC
# 4000011, Mediacom-Des Moines
# 4000012, Mediacom-Moline
# 4000013, Mediacom-Albany
# 4000200, Armstrong-Butler
# 8000150, Panhandle-Guymon
# 8000200, Blueridge-Palmerton
# 4000050, MidCo

declare -a arr=("8000150" "4000002")

 

# Run the aws s3 data getter 
AWS_ACCESS_KEY_ID="$access_key" AWS_SECRET_ACCESS_KEY="$access_secret" ./cdwdatagetter -r us-east-1 -b "$bucket" -d "$as_of" -p "$base_folder"

for provider in "${arr[@]}"
    do
    
    # get the latest file in the latest subdirectory for that provider
    for file in `ls -lad $base_folder/$provider/delta/*/* | awk -F ' '  ' { print $10 } ' | sort -r | head -1 `
        do  

            # check if file has been pulled before then don't not process
            if grep -q ${file} "$data_downloader_activity_tracker_file"; then
                echo " found file has been processed before  ${file}" >> $data_downloader_status_log_dir/cdw-data-downloader.log
                echo " found file has been processed before  ${file}"
                continue;
            fi

            echo " $(date). Getting file ${file}" >> $data_downloader_status_log_dir/cdw-data-downloader.log
            echo " $(date). Getting file ${file}"

            # bring the raw compressed file into the desired location.  Not sure if this is the correct awscli command
            # currently I brought the file to a local directories structure similar to the remote one on bucket rovi-cdw
            #aws s3 cp  ${file} $data_download_destination/

            # add the entry to the tracker and date
            echo " $(date)   ${file}" >>  $data_downloader_activity_tracker_file;

            # uncompress the the tv_viewership.cod.bz2 file
            # need to give the new path to the downloaded file


            gunzip -f ${file}
            # TODO - the line below wil not work as it looks for the original *.cod.bz file, 
            # TODO - while gzip delets the original *.cod.bz, and creates *.cod file
            # replace all Control A with Diamnonds
            cat -v "${file/.bz2/}" | sed 's/\^A/<>/g' > $data_download_destination/$diamonds_delimited_filename

            # create the subdirectory structure for today run and make it writeable
            mkdir $output_files_dir/$as_of;
            chmod a+rw $output_files_dir/$as_of;

            # create the subdirectory structure for provider and make it writeable
            mkdir $output_files_dir/$as_of/$provider;
            chmod a+rw $output_files_dir/$as_of/$provider;

            # create the csv report file for a given provider
            echo " creating csv file $output_files_dir/$as_of/$provider/hhid_count-$provider-$as_of.csv"
            touch $output_files_dir/$as_of/$provider/hhid_count-$provider-$as_of.csv

            # create the headings in the csv report file
            echo "date, provider_code, hh_id_count" >> $output_files_dir/$as_of/$provider/hhid_count-$provider-$as_of.csv

            # get unique household ids count when filtering other noise except channel tune events
            count=`cat -v  $data_download_destination/$diamonds_delimited_filename | grep "channel tune" | awk -F '<>' ' { print $25 }' | sort | uniq | wc -l `
            echo " count was completed for $provider ,$count"

            # write the result to csv report file
            echo "$as_of,$provider,$count" >> $output_files_dir/$as_of/$provider/hhid_count-$provider-$as_of.csv

            echo " deleting processed file $data_download_destination/$diamonds_delimited_filename after getting unique household ids count for $provider on $as_of "
            rm  $data_download_destination/$diamonds_delimited_filename

        done

        echo " cdw data downloader has finished processing the newest file ${file} for $provider  "
        echo " cdw data downloader has finished processing newest file: ${file}  for $provider" >> $data_downloader_status_log_dir/cdw-data-downloader.log

        sleep 1
done

echo " cdw data downloader has finished downloading files. "
echo " cdw data downloader has finished downloading files. " >> $data_downloader_status_log_dir/cdw-data-downloader.log
