From python

RUN apt-get update -y

ADD ./ /collector

WORKDIR /collector
# Python dependencies
RUN pip3 --no-cache-dir install -r requirement.txt
CMD [ "python", "/collector/code/collector.py" ]