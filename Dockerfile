FROM 998131032990.dkr.ecr.us-east-1.amazonaws.com/oracle-java8:latest

WORKDIR /opt/opentsdb-metron
# Add the build artifact under /opt, can be overridden by docker build
ARG ARTIFACT_PATH=opentsdb-2.3.0-fat.jar
ADD $ARTIFACT_PATH $WORKDIR
# default cmd
RUN apt-get update \
    && apt-get install --no-install-recommends -y --force-yes \
        gnuplot

CMD /opt/opentsdb-metron/run_in_container.sh
