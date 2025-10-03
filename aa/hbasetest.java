public class hbasetest {
    public static void main(String[] args) {
        DataStream<Order> orders = ...; // Kafka source

        orders.addSink(new HBaseSinkFunction("orders") {
            @Override
            public void invoke(Order value, Context context) throws Exception {
                String rowkey = hash(value.getCategory()).substring(0,4) + "_" + value.getDs() + "_" + value.getId();
                Put put = new Put(Bytes.toBytes(rowkey));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(value.getId()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("category"), Bytes.toBytes(value.getCategory()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("amount"), Bytes.toBytes(value.getAmount()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ts"), Bytes.toBytes(value.getTs()));
                put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ds"), Bytes.toBytes(value.getDs()));
                table.put(put);
            }
        });


    }
}
