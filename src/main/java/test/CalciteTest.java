package test;

import java.sql.*;

public class CalciteTest {

    public static void main(String[] args) {
        try {
            // Load the Calcite JDBC driver
            Class.forName("org.apache.calcite.jdbc.Driver");

            // Connect to the Calcite server
            try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=src/main/resources/my_model.json")) {
                System.out.println("Connected to Calcite server");


                // Execute SQL queries
                Statement statement = connection.createStatement();


                DatabaseMetaData md = connection.getMetaData();
                ResultSet tables = md.getTables(null, "cip", "%", null);
                while (tables.next()) {
                    System.out.println("--");
                    System.out.println(tables.getString(1));
                    System.out.println(tables.getString(2));
                    System.out.println(tables.getString(3));
                    System.out.println(tables.getString(4));
                    System.out.println("-->");
                }


                ResultSet resultSet = statement.executeQuery("SELECT * FROM \"ddw_dim_site\"");
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Process the query results
                while (resultSet.next()) {
                    // Process each row
                    for (int i = 1; i <= columnCount; i++) {
                        Object value = resultSet.getObject(i);
                        String columnName = metaData.getColumnName(i);
                        System.out.println(columnName + ": " + value);
                    }
                }

                // Close resources
                resultSet.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
