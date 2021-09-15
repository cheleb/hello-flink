FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY target/scala-2.12/root-assembly-0.1.0-SNAPSHOT.jar $FLINK_HOME/usrlib/root-assembly-0.1.0-SNAPSHOT.jar