package model;

import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;

public class InputJson {

    @SerializedName("envParallelism")
    Integer envParallelism;

    @SerializedName("delimiter")
    String delimiter;

    @SerializedName("tableName")
    String tableName;

    @SerializedName("tableColumn_1")
    String tableColumn_1;

    @SerializedName("tableColumn_2")
    String tableColumn_2;

    @SerializedName("tableColumn_3")
    String tableColumn_3;

    @SerializedName("operators")
    ArrayList<InputOperators> inputOperatorsList;

    public Integer getEnvParallelism() {
        return envParallelism;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTableColumn_1() {
        return tableColumn_1;
    }

    public String getTableColumn_2() {
        return tableColumn_2;
    }

    public String getTableColumn_3() {
        return tableColumn_3;
    }

    public ArrayList<InputOperators> getInputOperatorsList() {
        return (inputOperatorsList == null) ? new ArrayList<>() : inputOperatorsList;
    }

}