package model;

import com.google.gson.annotations.SerializedName;

public class InputOperators {

    @SerializedName("FunctionType")
    String functionType;

    @SerializedName("FunctionName")
    String functionName;

    @SerializedName("FunctionTopicSource")
    String functionTopicSource;

    @SerializedName("TableColumn_1")
    String tableColumn_1;

    @SerializedName("TableColumn_2")
    String tableColumn_2;

    @SerializedName("TableColumn_3")
    String tableColumn_3;

    @SerializedName("FunctionTopicSink")
    String functionTopicSink;

    @SerializedName("FilterValue")
    String filterValue;

    @SerializedName("FilterColumn")
    String filterColumn;

    @SerializedName("GroupByColumn")
    String groupByColumn;

    @SerializedName("AggregationColumn")
    String aggregationColumn;

    @SerializedName("JoinColumn_1")
    String fullOuterJoinColumn_1;

    @SerializedName("JoinColumn_2")
    String fullOuterJoinColumn_2;

    @SerializedName("SelectedJoinColumn_1")
    String selectedJoinColumn_1;

    @SerializedName("SelectedJoinColumn_2")
    String selectedJoinColumn_2;

    @SerializedName("ProjColumn1")
    String ProjColumn1;

    @SerializedName("ProjColumn2")
    String ProjColumn2;

    @SerializedName("GroupByWin")
    Boolean groupByWin;

    @SerializedName("GroupByTimeWin")
    int groupByTimeWin;

    @SerializedName("SavedForJoin")
    boolean savedForJoin;

    public String getFunctionType() {
        return functionType;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getFunctionTopicSource() {
        return functionTopicSource;
    }

    public String getFunctionTopicSink() {
        return functionTopicSink;
    }

    public String getFilterValue() {
        return filterValue;
    }

    public String getFilterColumn() {
        return filterColumn;
    }

    public String getGroupByColumn() {
        return groupByColumn;
    }

    public String getAggregationColumn() {
        return aggregationColumn;
    }

    public String getFullOuterJoinColumn_1() {
        return fullOuterJoinColumn_1;
    }

    public String getFullOuterJoinColumn_2() {
        return fullOuterJoinColumn_2;
    }

    public String getSelectedJoinColumn_1() {
        return selectedJoinColumn_1;
    }

    public String getSelectedJoinColumn_2() {
        return selectedJoinColumn_2;
    }

    public String getProjColumn1() {
        return ProjColumn1;
    }

    public String getProjColumn2() {
        return ProjColumn2;
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

    public Boolean getGroupByWin() {
        return groupByWin;
    }

    public int getGroupByTimeWin() {
        return groupByTimeWin;
    }

    public boolean getSavedForJoin() {
        return savedForJoin;
    }
}