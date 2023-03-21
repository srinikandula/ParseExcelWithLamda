package org.example;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class MySQLBatchInsertThread implements Runnable {
    private List<List<String>> columnValues;
    private DataSource ds;
    private String pstmtString;
    public MySQLBatchInsertThread(List<List<String>> columnValues, DataSource ds, String pstmtString){
        this.columnValues = columnValues;
        this.ds = ds;
        this.pstmtString = pstmtString;
    }
    @Override
    public void run() {
        try {
            long start = System.currentTimeMillis();
            Connection conn = ds.getConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(pstmtString);
            for(List<String> columnValuesList:columnValues){
                if(columnValuesList.size() != 0){
                    System.out.println(String.join(",", columnValuesList) +"  "+ columnValuesList.size());
                    int i = 1;
                    for(String columnValue : columnValuesList) {
                        preparedStatement.setString(i++, columnValue);
                    }
                    preparedStatement.addBatch();
                }
            }
            preparedStatement.executeBatch();
            preparedStatement.close();
            conn.close();
            long end = System.currentTimeMillis();
            System.out.println("Thread running done " + (end-start));
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }
}
