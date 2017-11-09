FROM 998131032990.dkr.ecr.us-east-1.amazonaws.com/oracle-java8:latest

WORKDIR /opt/opentsdb-metron
# Add the build artifact under /opt, can be overridden by docker build
ARG ARTIFACT_PATH=opentsdb-metron.tar.gz
ADD $ARTIFACT_PATH $WORKDIR

# default cmd
RUN apt-get update \
    && apt-get install --no-install-recommends -y --force-yes \
        gnuplot wget unzip

# Install yourkit agent.
RUN wget https://www.yourkit.com/download/YourKit-JavaProfiler-2017.02-b66.zip \
    && unzip YourKit-JavaProfiler-2017.02-b66.zip \
    && sudo mv YourKit-JavaProfiler-2017.02 /opt/yjp/

CMD ["./run_in_container.sh"]
