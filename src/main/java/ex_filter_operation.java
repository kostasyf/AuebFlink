

import com.google.gson.Gson;
import model.InputJson;
import model.InputOperators;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.Table;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import utils.FileResourcesUtils;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.table.api.Expressions.*;


public class ex_filter_operation {
    private static String FILTERCOLUMN;
    private static String FILTERVALUE;
    private static String FILTERNAMEOPERATOR;
    private static String FUNCTIONTOPICSOURCE;
    private static String FUNCTIONTOPICSINK;

    private static String GROUPBYCOLUMNNAME;
    private static String FUNCTIONNAME;

    private static String TABLECOLUMNNAME_1;
    private static String TABLECOLUMNNAME_2;
    private static String TABLECOLUMNNAME_3;

    private static String AGGREGATIONCOLUMN;
    private static String TABLE_1_JOINEDCOLUMN;
    private static String TABLE_2_JOINEDCOLUMN;
    private static String SELECTEDJOINCOLUMN_1;
    private static String SELECTEDJOINCOLUMN_2;


    private static String projCol1;
    private static String projCol2;
    private static int GLOBALCOUNTERTABLES =10;
    //private static boolean lastcell=false;
    private static Integer GROUPBYTIMEWIN=0;
    private static boolean GROUPBYWIN;
    private static boolean SAVEDFORJOIN;
    private static String SAVEDTABLE;

    public static Table doFilterJob(Table tableToFilter) {
        //filter with
        Table dofilterTable;
        if (FILTERNAMEOPERATOR.equals("FilterEq")) {
            dofilterTable = tableToFilter.filter($(FILTERCOLUMN).cast(Types.INT()).isEqual(Integer.parseInt(FILTERVALUE)));
        } else if (FILTERNAMEOPERATOR.equals("FilterGr")) {
            dofilterTable = tableToFilter.filter($(FILTERCOLUMN).cast(Types.INT()).isGreater(Integer.parseInt(FILTERVALUE)));
        } else if (FILTERNAMEOPERATOR.equals("FilterLess")) {
            dofilterTable = tableToFilter.filter($(FILTERCOLUMN).cast(Types.INT()).isLess(Integer.parseInt(FILTERVALUE)));
        } else {
            //FilterEqString
            dofilterTable = tableToFilter.filter($(FILTERCOLUMN).isEqual(FILTERVALUE));
        }

        return dofilterTable;
    }

    public static Table doGroupByJob(Table tableToGroupBy) {
        //GroupBy operator with sum windowed 1min
        Table doGroupByTable = null;

        //window is true
        if(GROUPBYWIN) {
            switch (FUNCTIONNAME) {
                case "groupby":
                    doGroupByTable = tableToGroupBy.window(Tumble.over(lit(GROUPBYTIMEWIN).minutes()).on($("proctime")).as("w")).groupBy($(GROUPBYCOLUMNNAME), $("w")).select($(GROUPBYCOLUMNNAME),
                            $("w").start(),
                            $("w").end(),
                            $("w").proctime());
                    break;
                case "groupbyCount":
                    doGroupByTable = tableToGroupBy.window(Tumble.over(lit(GROUPBYTIMEWIN).minutes()).on($("proctime")).as("w")).groupBy($(GROUPBYCOLUMNNAME), $("w")).select($(GROUPBYCOLUMNNAME),
                            $("w").start(),
                            $("w").end(),
                            $("w").proctime(),
                            $(AGGREGATIONCOLUMN).count().as("countTb"));
                    break;
                case "groupbySum":
                    doGroupByTable = tableToGroupBy.window(Tumble.over(lit(GROUPBYTIMEWIN).minutes()).on($("proctime")).as("w")).groupBy($(GROUPBYCOLUMNNAME), $("w")).select($(GROUPBYCOLUMNNAME),
                            $("w").start(),
                            $("w").end(),
                            $("w").proctime(),
                            $(AGGREGATIONCOLUMN).cast(Types.INT()).sum().as("countTb"));
                    break;
                case "groupbyAvg":
                    doGroupByTable = tableToGroupBy.window(Tumble.over(lit(GROUPBYTIMEWIN).minutes()).on($("proctime")).as("w")).groupBy($(GROUPBYCOLUMNNAME), $("w")).select($(GROUPBYCOLUMNNAME),
                            $("w").start(),
                            $("w").end(),
                            $("w").proctime(),
                            $(AGGREGATIONCOLUMN).cast(Types.INT()).avg().as("countTb"));
                    break;
                case "groupbyMax":
                    doGroupByTable = tableToGroupBy.window(Tumble.over(lit(GROUPBYTIMEWIN).minutes()).on($("proctime")).as("w")).groupBy($(GROUPBYCOLUMNNAME), $("w")).select($(GROUPBYCOLUMNNAME),
                            $("w").start(),
                            $("w").end(),
                            $("w").proctime(),
                            $(AGGREGATIONCOLUMN).cast(Types.INT()).max().as("countTb"));
                    break;
                case "groupbyMin":
                    doGroupByTable = tableToGroupBy.window(Tumble.over(lit(GROUPBYTIMEWIN).minutes()).on($("proctime")).as("w")).groupBy($(GROUPBYCOLUMNNAME), $("w")).select($(GROUPBYCOLUMNNAME),
                            $("w").start(),
                            $("w").end(),
                            $("w").proctime(),
                            $(AGGREGATIONCOLUMN).cast(Types.INT()).min().as("countTb"));
                    break;
                default   :
                    System.out.println( FUNCTIONNAME + "not found");
                    break;
            }
        }else {
            switch (FUNCTIONNAME){
                case "groupby" :
                    doGroupByTable = tableToGroupBy.groupBy($(GROUPBYCOLUMNNAME)).select($(GROUPBYCOLUMNNAME));
                    break;
                case "groupbyCount" :
                    doGroupByTable = tableToGroupBy.groupBy($(GROUPBYCOLUMNNAME)).select($(GROUPBYCOLUMNNAME),$(AGGREGATIONCOLUMN).count().as("countTb"));
                    break;
                case "groupbySum" :
                    doGroupByTable = tableToGroupBy.groupBy($(GROUPBYCOLUMNNAME)).select($(GROUPBYCOLUMNNAME),$(AGGREGATIONCOLUMN).cast(Types.LONG()).sum().as("countTb"));
                    break;
                case "groupbyAvg" :
                    doGroupByTable = tableToGroupBy.groupBy($(GROUPBYCOLUMNNAME)).select($(GROUPBYCOLUMNNAME),$(AGGREGATIONCOLUMN).cast(Types.LONG()).avg().as("countTb"));
                    break;
                case "groupbyMax" :
                    doGroupByTable = tableToGroupBy.groupBy($(GROUPBYCOLUMNNAME)).select($(GROUPBYCOLUMNNAME),$(AGGREGATIONCOLUMN).cast(Types.LONG()).max().as("countTb"));
                    break;
                case "groupbyMin" :
                    doGroupByTable = tableToGroupBy.groupBy($(GROUPBYCOLUMNNAME)).select($(GROUPBYCOLUMNNAME),$(AGGREGATIONCOLUMN).cast(Types.LONG()).min().as("countTb"));
                    break;
                default   :
                    System.out.println( FUNCTIONNAME + "not found");
                    break;
            }
        }

        return doGroupByTable;
    }

    public static Table doOuterJoinJob(Table tableLeft, Table tableRight) {
        Table tableJoin;
            tableJoin = tableLeft.join(tableRight).where($(TABLE_1_JOINEDCOLUMN).isEqual($(TABLE_2_JOINEDCOLUMN))).select($(SELECTEDJOINCOLUMN_1),$(SELECTEDJOINCOLUMN_2),$(TABLE_1_JOINEDCOLUMN),$("proctime"));
            //tableJoin = tableLeft.fullOuterJoin(tableRight, $(TABLE_1_JOINEDCOLUMN).isEqual($(TABLE_2_JOINEDCOLUMN))).select($(TABLE_1_JOINEDCOLUMN),$(SELECTEDJOINCOLUMN_1),$(SELECTEDJOINCOLUMN_2));
        return tableJoin;
    }

    public static Table doProjection(Table tProj, StreamTableEnvironment tableEnv){
        Table tableProj;
        tableProj = tableEnv.from("myTable"+GLOBALCOUNTERTABLES).select($(projCol1),$(projCol2));
        tableEnv.createTemporaryView("projectionTable", tableProj);
        return tableProj;
    }

    public  static void doFlinkOperator(String topicIN, StreamExecutionEnvironment env,StreamTableEnvironment tEnv, String topicSink, Properties properties, Properties prop,String fType, String delim){
        Table table1=null;
        Table tJobTable=null;
        //------------source-----------------------
        if (topicIN!=null) {
            FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topicIN, new SimpleStringSchema(), properties);
            consumer.setStartFromEarliest();
            //consumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60)));
            DataStream<String> ds = env.addSource(consumer);

            //-------String to Tuple3-------------------------


            DataStream<Tuple3<String, String, String>> dsT3 = ds.map(new SplitterKafkaString(delim));

            //convert dsT3 into Table with field
            GLOBALCOUNTERTABLES++;
            tEnv.createTemporaryView("myTable"+GLOBALCOUNTERTABLES,dsT3,$(TABLECOLUMNNAME_1), $(TABLECOLUMNNAME_2), $(TABLECOLUMNNAME_3),$("proctime").proctime());
            table1 = tEnv.from("myTable"+GLOBALCOUNTERTABLES);

        }else{
            table1 = tEnv.from("myTable"+GLOBALCOUNTERTABLES);

        }

        //-----------source IF JOIN----------------------
        /*
        if(fType.equals("Join")){
            //second kafka source
            String BOOTSTRAP_SERVER = "localhost:9092";
            Properties props = new Properties();
            props.put("bootstrap.servers", BOOTSTRAP_SERVER);
            props.put("group.id", "joinedonflink");
            FlinkKafkaConsumer<String> cmJoinSource = new FlinkKafkaConsumer<>(SOURCEJOINTOPIC, new SimpleStringSchema(), props);
            cmJoinSource.setStartFromEarliest();
            DataStream<String> dsJoinStream = env.addSource(cmJoinSource);

            DataStream<Tuple3<String, String, String>> dsJoinT3 = dsJoinStream.map(new SplitterKafkaString(delim));

            tEnv.createTemporaryView("myTable2",dsJoinT3,$(RIGHTABLECOLUMN_1), $(RIGHTABLECOLUMN_2), $(RIGHTABLECOLUMN_3));

        }*/

        //------------DO JOB Operator-----------------
        if (fType.equals("Filter"))
            tJobTable = doFilterJob(table1);
        else if (fType.equals("GroupBy"))
            tJobTable = doGroupByJob(table1);
        else if (fType.equals("Projection")) {
            tJobTable = doProjection(table1, tEnv);

        }else if(fType.equals("Join")){
            Table table2 = tEnv.from(SAVEDTABLE);
            Table table3 = table1.dropColumns($("proctime"));
            tJobTable = doOuterJoinJob(table2,table3);
        }
        GLOBALCOUNTERTABLES++;

        if(topicSink!=null) {
        //------------ConvertTable to dsT2<Boolean,Row>


            DataStream<Tuple2<Boolean,Row>> retractStreamJob = tEnv.toRetractStream(tJobTable,Row.class);
            DataStream<String> dsSink = retractStreamJob.map(new TupletoString()).rebalance();

        //--------------sink-----------------

            FlinkKafkaProducer<String> prKafka = new FlinkKafkaProducer<>(topicSink, new SimpleStringSchema(), prop);
            dsSink.addSink(prKafka);

        }else{
            //--------------keep tJobTable for next Operator--------------------
            tEnv.createTemporaryView("myTable"+GLOBALCOUNTERTABLES,tJobTable);
            if(SAVEDFORJOIN)
                SAVEDTABLE = "myTable"+GLOBALCOUNTERTABLES;
        }

    }

    public static void main(String[] args) throws Exception {

        //-------read JSON file------------

        InputJson inputJson = new Gson().fromJson(new FileResourcesUtils().getFileFromResourceAsString("json/temp.json"), InputJson.class);

        int GLOBAL_PARALLELISM = inputJson.getEnvParallelism();
        //set environment and table Environment
        //Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env,bsSettings);

        //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.setParallelism(GLOBAL_PARALLELISM);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));


        //init kafka connector properties
        String BOOTSTRAP_SERVER = "localhost:9092";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", BOOTSTRAP_SERVER);
        properties.put("group.id", "flinkconsole");
        Properties propSink = new Properties();
        propSink.put("bootstrap.servers", BOOTSTRAP_SERVER);

        String DELIMITER = inputJson.getDelimiter();


        //----------------DoFlinkJOB---------------------



        for (InputOperators inputOperators : inputJson.getInputOperatorsList()) {
            FUNCTIONTOPICSOURCE = inputOperators.getFunctionTopicSource();
            FUNCTIONTOPICSINK = inputOperators.getFunctionTopicSink();
            FUNCTIONNAME = inputOperators.getFunctionName();
            SAVEDFORJOIN = inputOperators.getSavedForJoin();

            if(FUNCTIONTOPICSOURCE !=null){
                TABLECOLUMNNAME_1 = inputOperators.getTableColumn_1();
                TABLECOLUMNNAME_2 = inputOperators.getTableColumn_2();
                TABLECOLUMNNAME_3 = inputOperators.getTableColumn_3();
            }

            switch (inputOperators.getFunctionType()) {
                case "Projection":

                    projCol1= inputOperators.getProjColumn1();
                    projCol2= inputOperators.getProjColumn2();
                    doFlinkOperator(FUNCTIONTOPICSOURCE,env,tEnv,FUNCTIONTOPICSINK,properties,propSink,inputOperators.getFunctionType(),DELIMITER);

                    break;

                case "Filter":

                    FILTERNAMEOPERATOR = inputOperators.getFunctionName();
                    FILTERCOLUMN = inputOperators.getFilterColumn();
                    FILTERVALUE = inputOperators.getFilterValue();
                    doFlinkOperator(FUNCTIONTOPICSOURCE,env,tEnv,FUNCTIONTOPICSINK,properties,propSink,inputOperators.getFunctionType(),DELIMITER);

                    break;
                case "GroupBy":
                    GROUPBYWIN = inputOperators.getGroupByWin();
                    if(GROUPBYWIN)
                        GROUPBYTIMEWIN = inputOperators.getGroupByTimeWin();
                    if (GROUPBYTIMEWIN==0)
                        GROUPBYTIMEWIN=1;//default value

                    GROUPBYCOLUMNNAME = inputOperators.getGroupByColumn();
                    AGGREGATIONCOLUMN = inputOperators.getAggregationColumn();
                    doFlinkOperator(FUNCTIONTOPICSOURCE,env,tEnv,FUNCTIONTOPICSINK,properties,propSink,inputOperators.getFunctionType(),DELIMITER);

                    break;
                case "Join":


                    TABLE_1_JOINEDCOLUMN = inputOperators.getFullOuterJoinColumn_1();
                    TABLE_2_JOINEDCOLUMN = inputOperators.getFullOuterJoinColumn_2();
                    SELECTEDJOINCOLUMN_1 = inputOperators.getSelectedJoinColumn_1();
                    SELECTEDJOINCOLUMN_2 = inputOperators.getSelectedJoinColumn_2();

                    doFlinkOperator(FUNCTIONTOPICSOURCE,env,tEnv,FUNCTIONTOPICSINK,properties,propSink,inputOperators.getFunctionType(),DELIMITER);

                    break;
                default:
                    System.out.println("operator " + inputOperators.getFunctionType() + "not found");
                    break;
            }
        }



        //System.out.println(env.getExecutionPlan());

        env.execute();

    }


    public static class SplitterKafkaString implements MapFunction<String, Tuple3<String, String, String>> {
        @Override
        public Tuple3<String, String, String> map(String s) throws Exception {
            try {
                String[] cells = s.split(delimiter);


                    return new Tuple3<>(cells[0], cells[1], cells[2]);
            }catch(Exception e){
                return new Tuple3<>("a","s","12");}
        }
        String delimiter;
        public SplitterKafkaString(String delimiter){
            this.delimiter = delimiter;
        }
    }


    public static class TupletoString implements MapFunction<Tuple2<Boolean,Row>,String>{

        @Override
        public String map(Tuple2<Boolean,Row> tuple2BR) throws Exception {
            return tuple2BR.f0.toString()+","+tuple2BR.f1.toString();
        }
    }


}

