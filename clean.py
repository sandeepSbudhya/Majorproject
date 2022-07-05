from pyspark.sql import SparkSession



spark = SparkSession.builder.config("spark.executor.memory", "5g").config("spark.driver.memory", "15g").getOrCreate()
df = spark.read.csv('testdata/test2.csv', sep=',', inferSchema=True, header=True).drop(' Flow Duration', ' Fwd Header Length55')\
.withColumnRenamed(' Destination Port','Dst Port') \
.withColumnRenamed(' Total Fwd Packets', 'Tot Fwd Pkts') \
.withColumnRenamed(' Total Backward Packets', 'Tot Bwd Pkts') \
.withColumnRenamed('Total Length of Fwd Packets', 'TotLen Fwd Pkts') \
.withColumnRenamed(' Total Length of Bwd Packets', 'TotLen Bwd Pkts') \
.withColumnRenamed(' Fwd Packet Length Max', 'Fwd Pkt Len Max')\
.withColumnRenamed(' Fwd Packet Length Min', 'Fwd Pkt Len Min') \
.withColumnRenamed(' Fwd Packet Length Mean', 'Fwd Pkt Len Mean') \
.withColumnRenamed(' Fwd Packet Length Std', 'Fwd Pkt Len Std')\
.withColumnRenamed('Bwd Packet Length Max', 'Bwd Pkt Len Max')\
.withColumnRenamed(' Bwd Packet Length Min', 'Bwd Pkt Len Min') \
.withColumnRenamed(' Bwd Packet Length Mean', 'Bwd Pkt Len Mean') \
.withColumnRenamed(' Bwd Packet Length Std', 'Bwd Pkt Len Std')\
.withColumnRenamed('Flow Bytes/s', 'Flow Byts/s')\
.withColumnRenamed(' Flow Packets/s', 'Flow Pkts/s')\
.withColumnRenamed(' Flow IAT Mean', 'Flow IAT Mean') \
.withColumnRenamed(' Flow IAT Std', 'Flow IAT Std')\
.withColumnRenamed(' Flow IAT Max', 'Flow IAT Max') \
.withColumnRenamed(' Flow IAT Min', 'Flow IAT Min') \
.withColumnRenamed('Fwd IAT Total', 'Fwd IAT Tot')\
.withColumnRenamed(' Fwd IAT Mean', 'Fwd IAT Mean') \
.withColumnRenamed(' Fwd IAT Std', 'Fwd IAT Std')\
.withColumnRenamed(' Fwd IAT Max', 'Fwd IAT Max') \
.withColumnRenamed(' Fwd IAT Min', 'Fwd IAT Min')\
.withColumnRenamed('Bwd IAT Total', 'Bwd IAT Tot') \
.withColumnRenamed(' Bwd IAT Mean', 'Bwd IAT Mean') \
.withColumnRenamed(' Bwd IAT Std', 'Bwd IAT Std')\
.withColumnRenamed(' Bwd IAT Max', 'Bwd IAT Max') \
.withColumnRenamed(' Bwd IAT Min', 'Bwd IAT Min') \
.withColumnRenamed('Fwd PSH Flags', 'Fwd PSH Flags') \
.withColumnRenamed(' Bwd PSH Flags', 'Bwd PSH Flags') \
.withColumnRenamed(' Fwd URG Flags', 'Fwd URG Flags') \
.withColumnRenamed(' Bwd URG Flags', 'Bwd URG Flags') \
.withColumnRenamed(' Fwd Header Length34', 'Fwd Header Len') \
.withColumnRenamed(' Bwd Header Length', 'Bwd Header Len')\
.withColumnRenamed('Fwd Packets/s', 'Fwd Pkts/s')\
.withColumnRenamed(' Bwd Packets/s', 'Bwd Pkts/s')\
.withColumnRenamed(' Min Packet Length', 'Pkt Len Min') \
.withColumnRenamed(' Max Packet Length', 'Pkt Len Max')\
.withColumnRenamed(' Packet Length Mean', 'Pkt Len Mean')\
.withColumnRenamed(' Packet Length Std', 'Pkt Len Std')\
.withColumnRenamed(' Packet Length Variance', 'Pkt Len Var')\
.withColumnRenamed('FIN Flag Count', 'FIN Flag Cnt')\
.withColumnRenamed(' SYN Flag Count', 'SYN Flag Cnt')\
.withColumnRenamed(' RST Flag Count', 'RST Flag Cnt')\
.withColumnRenamed(' PSH Flag Count', 'PSH Flag Cnt')\
.withColumnRenamed(' ACK Flag Count', 'ACK Flag Cnt')\
.withColumnRenamed(' URG Flag Count', 'URG Flag Cnt')\
.withColumnRenamed(' CWE Flag Count', 'CWE Flag Count')\
.withColumnRenamed(' ECE Flag Count', 'ECE Flag Cnt')\
.withColumnRenamed(' Down/Up Ratio', 'Down/Up Ratio')\
.withColumnRenamed(' Average Packet Size', 'Pkt Size Avg')\
.withColumnRenamed(' Avg Fwd Segment Size', 'Fwd Seg Size Avg')\
.withColumnRenamed(' Avg Bwd Segment Size', 'Bwd Seg Size Avg')\
.withColumnRenamed('Fwd Avg Bytes/Bulk', 'Fwd Byts/b Avg')\
.withColumnRenamed(' Fwd Avg Packets/Bulk', 'Fwd Pkts/b Avg')\
.withColumnRenamed(' Fwd Avg Bulk Rate', 'Fwd Blk Rate Avg')\
.withColumnRenamed(' Bwd Avg Bytes/Bulk', 'Bwd Byts/b Avg')\
.withColumnRenamed(' Bwd Avg Packets/Bulk', 'Bwd Pkts/b Avg')\
.withColumnRenamed('Bwd Avg Bulk Rate', 'Bwd Blk Rate Avg')\
.withColumnRenamed('Subflow Fwd Packets', 'Subflow Fwd Pkts')\
.withColumnRenamed(' Subflow Fwd Bytes', 'Subflow Fwd Byts')\
.withColumnRenamed(' Subflow Bwd Packets', 'Subflow Bwd Pkts')\
.withColumnRenamed(' Subflow Bwd Bytes', 'Subflow Bwd Byts')\
.withColumnRenamed('Init_Win_bytes_forward', 'Init Fwd Win Byts')\
.withColumnRenamed(' Init_Win_bytes_backward', 'Init Bwd Win Byts')\
.withColumnRenamed(' act_data_pkt_fwd', 'Fwd Act Data Pkts')\
.withColumnRenamed(' min_seg_size_forward', 'Fwd Seg Size Min')\
.withColumnRenamed('Active Mean', 'Active Mean')\
.withColumnRenamed(' Active Std', 'Active Std')\
.withColumnRenamed(' Active Max', 'Active Max') \
.withColumnRenamed(' Active Min', 'Active Min') \
.withColumnRenamed('Idle Mean', 'Idle Mean')\
.withColumnRenamed(' Idle Std', 'Idle Std')\
.withColumnRenamed(' Idle Max', 'Idle Max')\
.withColumnRenamed(' Idle Min', 'Idle Min')
df.printSchema()
df.write.option("header",True).csv("cleaned2017file2.csv")