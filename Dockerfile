From python

RUN apt-get update -y

ADD ./data_collection/ /collector
ADD requirements.txt /collector
ADD main.sh /collector
RUN chmod -R 777 /collector/main.sh
WORKDIR /collector
# Python dependencies
RUN pip3 --no-cache-dir install -r requirements.txt
CMD [ "nohup", "/collector/main.sh","&" ]
