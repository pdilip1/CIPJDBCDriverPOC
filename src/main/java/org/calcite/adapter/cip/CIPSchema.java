package org.calcite.adapter.cip;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.model.JsonTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.cip.CIPFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.cip.TableDefinition;
import org.cip.TableDefinitions;
import org.cip.ColumnDefinition;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * CIPSchema class
 */
public class CIPSchema extends AbstractSchema {

    Logger logger = LoggerFactory.getLogger(CIPSchema.class);

    private final String group;

    private Map<String, Table> tableMap;

    private final ObjectMapper objectMapper = new ObjectMapper();


    public CIPSchema(String group) {
        this.group = group;
    }

    /**
     * Gets the table map under this schema
     * @return Map of TableName and Table object
     */
    @Override protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            TableDefinitions tableDefinitions = readTableMetadata();
            tableMap = createTableMap(tableDefinitions);
        }
        return tableMap;
    }


    TableDefinitions readTableMetadata()
    {
        File file = new File("src/main/resources/table_metadata.json");

        // Create ObjectMapper
        ObjectMapper mapper = new ObjectMapper();

        // Deserialize JSON to TableDefinitions object
        TableDefinitions tableDefinitions = null;
        try {
            tableDefinitions = mapper.readValue(file, TableDefinitions.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return tableDefinitions;

/*
        // Print table definitions
        for (TableDefinition table : tableDefinitions.getTables()) {
            System.out.println("Table Name: " + table.getName());
            System.out.println("Table Alias: " + table.getAlias());
            System.out.println("Columns:");
            for (ColumnDefinition column : table.getColumns()) {
                System.out.println("\tColumn Name: " + column.getName());
                System.out.println("\tColumn Alias: " + column.getAlias());
            }
            System.out.println();
        }
*/
    }

    /**
     * Creates the table map for the specified CIP group
     * @return Map of tableName and CIPScannableTable
     */
    private Map<String, Table> createTableMap(TableDefinitions tableDefinitions) {

        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        logger.debug(String.format("getting tables for group: '%s'", this.group));

        /**
         * List all tables under different categories in CIP
         */
        if (this.group.equals("cip")) {

            for (TableDefinition tableDefinition : tableDefinitions.getTables())
            {
                CIPScannableTable table = (CIPScannableTable) createTable(tableDefinition);
                // Note: The table names are provided as alias names (i.e. getTableAlias)
                builder.put(table.getTableAlias().toUpperCase(Locale.getDefault()), table);
            }
        }
        return builder.build();
    }

    private Table createTable (TableDefinition tableDefinition) {

        return new CIPScannableTable(tableDefinition);
    }
}
