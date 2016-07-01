FROM       ubuntu
MAINTAINER Gevorg Gevorgyan <gevgev@yahoo.com>

#ENV AWS_ACCESS_KEY_ID
#ENV AWS_SECRET_ACCESS_KEY
RUN apt-get update \
  && apt-get upgrade -y 

RUN apt-get install -y ca-certificates

RUN apt-get install -y bzip2  

#RUN apt-get install -y \
#    ssh \
#    python \
#    python-pip \
#    python-virtualenv

#RUN \
#    mkdir aws && \
#    virtualenv aws/env && \
#    ./aws/env/bin/pip install awscli && \
#    echo 'source $HOME/aws/env/bin/activate' >> .bashrc && \
#    echo 'complete -C aws_completer aws' >> .bashrc

#RUN $HOME/aws/env/bin/aws

ADD cdwdatagetter cdwdatagetter 
ADD aws-s3-uploader aws-s3-uploader
ADD run-ubuntu.sh run-ubuntu.sh
ADD mso-list.csv mso-list.csv

ARG DATE

ENTRYPOINT ./run-ubuntu.sh $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY rovi-cdw data_downloader_tracker.txt cdw_downloads_logs input_compressed_cdw_data cdw-data-reports tv_viewership.cod event/tv_viewership mso-list.csv $DATE
