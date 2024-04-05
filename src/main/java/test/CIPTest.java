package test;

import com.google.common.collect.Ordering;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.*;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class CIPTest {

    /**
     * Run against Avatica server
     * @throws SQLException
     */
    @Test
    public void testSelectView() throws SQLException {
        String sql = "select \"metric_id_a\", \"metric_value_a\" from realtime_metric";
        //String sql = "INSERT INTO src_code_grp VALUES (12, TRUE)";
        Connection connection = null;
        Statement statement = null;
        try {
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:9787;serialization=PROTOBUF");
            statement = connection.createStatement();
            try {
                final ResultSet resultSet =
                        statement.executeQuery(
                                sql);
                output(resultSet);
            }catch (Exception e)
            {
                e.printStackTrace();
                throw e;
            }

        } finally {
            close(connection, statement);
        }

    }

    /**
     * Run directly against Calcite model
     * @throws SQLException
     */
    @Test
    public void testSelectView1() throws SQLException {
        //sql("model", "select * from ddw_fact_realtime_metric").ok();
        Connection connection = null;
        Statement statement = null;
        try {
            String sql = "select \"is_enabled_a\", \"source_code_group_id_a\" from src_code_grp";
            Class.forName("org.apache.calcite.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:calcite:model=src/main/resources/model.json");
            statement = connection.createStatement();
            try {
                final ResultSet resultSet =
                        statement.executeQuery(
                                sql);
                output(resultSet);
            }catch (Exception e)
            {
                e.printStackTrace();
                throw e;
            }


        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        finally {
            close(connection, statement);
        }

    }


    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        return Sources.of(CIPTest.class.getResource("/" + path)).file().getAbsolutePath();
    }

    private Fluent sql(String model, String sql) {
        return new Fluent(model, sql, this::output);
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        out.print("---------------------------------");
        out.println();
        for (int i = 1;; i++) {
            out.print(metaData.getColumnLabel(i));
            if (i < columnCount) {
                out.print(", ");
            } else {
                out.println();
                break;
            }
        }
        out.println("---------------------------------");

        while (resultSet.next()) {
            for (int i = 1;; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    private Void output(ResultSet resultSet) {
        try {
            output(resultSet, System.out);
        } catch (SQLException e) {
            //throw TestUtil.rethrow(e);
        }
        return null;
    }

    private void checkSql(String sql, String model, Consumer<ResultSet> fn)
            throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
//            Properties info = new Properties();
//            info.put("model", jsonPath(model));
            connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://localhost:55332;serialization=PROTOBUF");
            statement = connection.createStatement();
            try {
                final ResultSet resultSet =
                        statement.executeQuery(
                                sql);
                fn.accept(resultSet);
            }catch (Exception e)
            {
                e.printStackTrace();
            }

        } finally {
            close(connection, statement);
        }
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    /** Returns a function that checks the contents of a result set against an
     * expected string. */
    private static Consumer<ResultSet> expect(final String... expected) {
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                CIPTest.collect(lines, resultSet);
                assertEquals(Arrays.asList(expected), lines);
            } catch (SQLException e) {
                e.printStackTrace();
                //throw TestUtil.rethrow(e);
            }
        };
    }

    private static void collect(List<String> result, ResultSet resultSet)
            throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            buf.setLength(0);
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for (int i = 1; i <= n; i++) {
                buf.append(sep)
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getString(i));
                sep = "; ";
            }
            result.add(Util.toLinux(buf.toString()));
        }
    }

    /** Returns a function that checks the contents of a result set against an
     * expected string. */
    private static Consumer<ResultSet> expectUnordered(String... expected) {
        final List<String> expectedLines =
                Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                CIPTest.collect(lines, resultSet);
                Collections.sort(lines);
                assertEquals(expectedLines, lines);
            } catch (SQLException e) {
                e.printStackTrace();
               // throw TestUtil.rethrow(e);
            }
        };
    }


    /** Fluent API to perform test actions. */
    private class Fluent {
        private final String model;
        private final String sql;
        private final Consumer<ResultSet> expect;

        Fluent(String model, String sql, Consumer<ResultSet> expect) {
            this.model = model;
            this.sql = sql;
            this.expect = expect;
        }

        /** Runs the test. */
        Fluent ok() {
            try {
                checkSql(sql, model, expect);
                return this;
            } catch (SQLException e) {
                //throw TestUtil.rethrow(e);
            }
            return null;
        }

        /** Assigns a function to call to test whether output is correct. */
        Fluent checking(Consumer<ResultSet> expect) {
            return new Fluent(model, sql, expect);
        }

        /** Sets the rows that are expected to be returned from the SQL query. */
        Fluent returns(String... expectedLines) {
            return checking(expect(expectedLines));
        }

        /** Sets the rows that are expected to be returned from the SQL query,
         * in no particular order. */
        Fluent returnsUnordered(String... expectedLines) {
            return checking(expectUnordered(expectedLines));
        }
    }
}

