# hbase_kerberos_pigudf

Used for a kerberos test from java client directly -> HbaseTestNew.java



javac -cp .:/usr/lib/hbaslib/*:/usr/lib/hbase/e/hbase-client-1.4.9.jar:$(hadoop classpath) com/pigudf/HbaseTestNew.java
java -cp .:/usr/lib/hbaslib/*:/usr/lib/hbase/e/hbase-client-1.4.9.jar:$(hadoop classpath) com.pigudf.HbaseTestNew

one pig very simple udf test -> GetClassification.java


and pig hbase udf test -> GetHbaseRow.java

#javac -cp /usr/lib/hbase/hbase-client-1.4.9.jar:/usr/lib/hadoop/hadoop-common-2.8.5-amzn-3.jar:/usr/lib/hbase/hbase-common-1.4.9.jar:/usr/lib/pig/pig-0.17.0-core-h2.jar:/usr/lib/hadoop/client/log4j-1.2.17.jar:/usr/lib/hadoop/client/commons-logging-1.1.3.jar com/pigudf/GetHbaseRow.java
javac -cp .:/usr/lib/hbase/*:/usr/lib/hbase/lib/*:$(hadoop classpath):/usr/lib/pig/pig-0.17.0-core-h2.jar com/pigudf/GetHbaseRow.java

jar cf gethbaserow.jar com/pigudf/GetHbaseRow.class



the Java client version, this is sampe function as GetHbaseRow, just move from udf to client-> HbaseRow.java

javac -cp .:/usr/lib/hbase/*:/usr/lib/hbase/lib/*:$(hadoop classpath) com/pigudf/HbaseRow.java

java -cp .:/usr/lib/hbase/*:/usr/lib/hbase/lib/*:$(hadoop classpath) com.pigudf.HbaseRow '1990' 'test' 'cf' 'status'
