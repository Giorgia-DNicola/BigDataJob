FROM bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8

ENV CORE_CONF_fs_defaultFS=hdfs://namenode:8020
ENV CORE_CONF_hadoop_http_staticuser_user=root
ENV HDFS_CONF_dfs_replication=1

VOLUME /hadoop/dfs/data
