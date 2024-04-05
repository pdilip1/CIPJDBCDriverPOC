package org.calcite.adapter.cip;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;
import org.cip.CIPFieldType;
import org.cip.ColumnDefinition;
import org.cip.TableDefinition;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CIPScannableTable extends AbstractTable
        implements ScannableTable {

    private static final String POSTGRESQL_SCHEMA = "PUBLIC";
    private String tableName;
    private String tableAlias;

    protected List<String> fieldNames;

    protected List<String> fieldAliasNames;

    List<CIPFieldType> cipFieldTypes;

    private List<RelDataType> fieldTypes = new ArrayList<>();

    TableDefinition tableDefinition;

    public CIPScannableTable(TableDefinition tableDefinition) {
        this.tableDefinition = tableDefinition;
        this.tableName = tableDefinition.getName();
        this.tableAlias = tableDefinition.getAlias();
        this.fieldNames = new ArrayList<>();
        this.fieldAliasNames = new ArrayList<>();
        this.cipFieldTypes =  new ArrayList<>();
        getColumnNames(tableDefinition.getColumns(), fieldNames, fieldAliasNames, cipFieldTypes);
    }

    private void getColumnNames (List<ColumnDefinition> columnDefinitions, List<String> fieldNames, List<String> fieldAliasNames, List<CIPFieldType> cipFieldTypes)
    {
        for (ColumnDefinition columnDefinition : columnDefinitions)
        {
            fieldAliasNames.add(columnDefinition.getAlias());
            fieldNames.add(columnDefinition.getName());
            cipFieldTypes.add(CIPFieldType.of(columnDefinition.getType()));
        }
    }

    public String getTableAlias() {
        return this.tableAlias;
    }

    public String getTableName() {
        return this.tableName;
    }

    public Enumerable<Object[]> scan(DataContext dataContext) {
        try {

            Connection connection = DriverManager.getConnection("jdbc:calcite:");

            // Unwrap our connection using the CalciteConnection
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);

            // Get a pointer to our root schema for our Calcite Connection
            SchemaPlus rootSchema = calciteConnection.getRootSchema();

            // Instantiate a data source, this can be autowired in using Spring as well
            DataSource postgresDataSource = JdbcSchema.dataSource(
                    "jdbc:postgresql://localhost:5432/cip_user_db",
                    "org.postgresql.Driver", // Change this if you want to use something like MySQL, Oracle, etc.
                    "postgres", // username
                    "postgres"   // password
            );


            // Attach our Postgres Jdbc Datasource to CIP Root Schema
            rootSchema.add(POSTGRESQL_SCHEMA, JdbcSchema.create(rootSchema, POSTGRESQL_SCHEMA, postgresDataSource, null, null));

            // Convert each string in the list to a string with double quotes around it
            List<String> quotedStrings = fieldNames.stream()
                    .map(s -> "\"" + s + "\"")
                    .collect(Collectors.toList());

            String commaSeparatedString = String.join(", ", quotedStrings);

            String query = "SELECT " +  commaSeparatedString  +   " FROM public." + "\"" + tableName + "\"";

            Supplier<Connection> connectionSupplier = () -> connection;
            return new LazyFetchingEnumerable(connectionSupplier, query);

        }catch (Exception e)
        {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Get fields and their types in a row
     * @param typeFactory
     * @return
     */
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        for (CIPFieldType cipFieldType : cipFieldTypes)
        {
            switch(cipFieldType)
            {
                case STRING:
                    fieldTypes.add(CIPFieldType.STRING.toType((JavaTypeFactory) typeFactory));
                    break;

                case BOOLEAN:
                    fieldTypes.add(CIPFieldType.BOOLEAN.toType((JavaTypeFactory) typeFactory));
                    break;
            }
        }

        // Note: The column names are provided as alias names (i.e. fieldAliasNames)
        return typeFactory.createStructType(Pair.zip(fieldAliasNames, fieldTypes));
    }

    // Custom enumerable for lazy fetching
    private static class LazyFetchingEnumerable extends AbstractEnumerable<Object[]> {
        private final Supplier<Connection> connectionSupplier;
        private final String query;

        public LazyFetchingEnumerable(Supplier<Connection> connectionSupplier, String query) {
            this.connectionSupplier = connectionSupplier;
            this.query = query;
        }

        @Override
        public Enumerator<Object[]> enumerator() {
            return new LazyFetchingEnumerator(connectionSupplier, query);
        }
    }

    // Custom enumerator for lazy fetching
    private static class LazyFetchingEnumerator implements Enumerator<Object[]> {
        private Connection connection;
        private Statement statement;
        private ResultSet resultSet;

        public LazyFetchingEnumerator(Supplier<Connection> connectionSupplier, String query) {
            try {
                connection = connectionSupplier.get();
                statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                resultSet = statement.executeQuery(query);
                resultSet.setFetchSize(Integer.MIN_VALUE); // Enable streaming result set
            } catch (SQLException e) {
                throw new RuntimeException("Error creating lazy fetching enumerator", e);
            }
        }

        @Override
        public Object[] current() {
            try {
                // Read data from the current row of the result set
                Object[] rowData = new Object[resultSet.getMetaData().getColumnCount()];
                for (int i = 0; i < rowData.length; i++) {
                    rowData[i] = resultSet.getObject(i + 1); // ResultSet indices are 1-based
                }
                return rowData;
            } catch (SQLException e) {
                throw new RuntimeException("Error fetching data from result set", e);
            }
        }

        @Override
        public boolean moveNext() {
            try {
                // Move to the next row
                return resultSet.next();
            } catch (SQLException e) {
                throw new RuntimeException("Error moving to next row", e);
            }
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException("Reset operation not supported");
        }

        @Override
        public void close() {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new RuntimeException("Error closing resources", e);
            }
        }
    }
}
