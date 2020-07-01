Устаналиваем Ubuntu server 18-04
Обновляем репозиторий
sudo apt update
sudo apt upgrade
Файл для глобальных переменных системы (переменная PATH определяется в sudoers при использовании su или sudo)
sudo touch /etc/profile.d/global-env.sh && sudo chmod a+w /etc/profile.d/global-env.sh

JAVA
sudo apt install openjdk-8-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> /etc/profile.d/global-env.sh
echo 'export PATH=$PATH:$JAVA_HOME/bin' >> /etc/profile.d/global-env.sh 

HDP
скачиваем и распаковываем дистрибутив в папку /opt
sudo wget -P /opt https://archive.apache.org/dist/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz
sudo tar -C /opt -xzf /opt/hadoop-2.7.7.tar.gz && sudo rm /opt/hadoop-2.7.7.tar.gz

echo 'export HADOOP_HOME=/opt/hadoop-2.7.7' >> /etc/profile.d/global-env.sh
echo 'export PATH=$PATH:$HADOOP_HOME/bin' >> /etc/profile.d/global-env.sh
echo 'export PATH=$PATH:$HADOOP_HOME/sbin' >> /etc/profile.d/global-env.sh
echo 'export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop' >> /etc/profile.d/global-env.sh

echo 'export HADOOP_LOG_DIR=/home/hadoop/logs' >> /opt/hadoop-2.7.7/etc/hadoop/hadoop-env.sh
echo 'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64' >> /opt/hadoop-2.7.7/etc/hadoop/hadoop-env.sh

echo 'export YARN_LOG_DIR=/home/hadoop/logs' >> /opt/hadoop-2.7.7/etc/hadoop/yarn-env.sh

Добавить в конфигурационные файлы следующие настройки
$HADOOP_HOME/etc/hadoop/core-site.xml:
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>

$HADOOP_HOME/etc/hadoop/hdfs-site.xml:
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/home/hadoop/name</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/home/hadoop/data</value>
    </property>
</configuration>

cp $HADOOP_HOME/etc/hadoop/mapred-site.xml.template $HADOOP_HOME/etc/hadoop/mapred-site.xml
$HADOOP_HOME/etc/hadoop/mapred-site.xml:
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
</configuration>

$HADOOP_HOME/etc/hadoop/yarn-site.xml:
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>1024</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>1024</value>
    </property>
    <property>
        <name>yarn.nodemanager.vmem-check-enabled</name>
        <value>false</value>
    </property>
</configuration>

sudo adduser hadoop
sudo -u hadoop mkdir /home/hadoop/data
sudo -u hadoop mkdir /home/hadoop/name
sudo -u hadoop mkdir /home/hadoop/logs

Добавляем ssh ключ
sudo -u hadoop ssh-keygen -t rsa -P '' -f /home/hadoop/.ssh/id_rsa
sudo -u hadoop cp /home/hadoop/.ssh/id_rsa.pub /home/hadoop/.ssh/authorized_keys

форматируем фс
sudo -u hadoop /opt/hadoop-2.7.7/bin/hdfs namenode -format
запускаем сервисы
sudo -u hadoop /opt/hadoop-2.7.7/sbin/start-all.sh

hdfs dfs -mkdir /user
hdfs dfs -chmod 777 /user
hdfs dfs -mkdir /user/stream
hdfs dfs -chmod 777 /user/stream

SPARK
sudo wget -P /opt https://apache-mirror.rbc.ru/pub/apache/spark/spark-2.4.6/spark-2.4.6-bin-hadoop2.7.tgz
sudo tar -C /opt -xzf /opt/spark-2.4.6-bin-hadoop2.7.tgz && sudo rm /opt/spark-2.4.6-bin-hadoop2.7.tgz
echo 'export SPARK_HOME=/opt/spark-2.4.6-bin-hadoop2.7' >> /etc/profile.d/global-env.sh
echo 'export PATH=$PATH:$SPARK_HOME/bin' >> /etc/profile.d/global-env.sh

sudo adduser spark
sudo -u spark mkdir /home/spark/stream
sudo -u spark mkdir /home/spark/stream/tmp

POSTGRESQL
sudo apt install postgresql-10
поправить конфиги
/etc/postgresql/10/main/pg_hba.conf
есть
local   all             all                                     peer
должно быть
local   all             all                                     md5
перезагрузить сервис
sudo service postgresql restart

создать роль, базу и таблицу
sudo -u postgres psql
create user spark password 'spark';
create database spark owner spark;

psql -U spark
create table stream (skill varchar(100), cnt integer, snap_date integer);

