FROM python:3.9.16
RUN apt-get update
RUN apt-get install -y vim

RUN curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o $HOME/minio-binaries/mc

RUN chmod +x $HOME/minio-binaries/mc
RUN mv $HOME/minio-binaries/mc /bin/mc

RUN pip install boto3 pymongo
RUN mkdir /s3transfer
COPY *.py /s3transfer/
COPY *.ini /s3transfer/
WORKDIR /s3transfer
ENTRYPOINT ["/bin/bash", "-c"]
CMD "/bin/sleep"
