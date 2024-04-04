package org.calcite.adapter.cip;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.cip.CIPFieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * CIPSchema class
 */
public class CIPSchema extends AbstractSchema {

    Logger logger = LoggerFactory.getLogger(CIPSchema.class);

    private final String group;

    private Map<String, Table> tableMap;


    public CIPSchema(String group) {
        this.group = group;
    }

    /**
     * Gets the table map under this schema
     * @return Map of TableName and Table object
     */
    @Override protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    /**
     * Creates the table map for the specified CIP group
     * @return Map of tableName and CIPScannableTable
     */
    private Map<String, Table> createTableMap() {

        final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();

        logger.debug(String.format("getting tables for group: '%s'", this.group));

        /**
         * List all tables under different categories in CIP
         */
        if (this.group.equals("cip")) {

            List<String> fieldNames = Arrays.asList("metric_id", "metric_value");
            List<CIPFieldType> fieldTypes = Arrays.asList(CIPFieldType.STRING, CIPFieldType.STRING);
            CIPScannableTable table = (CIPScannableTable) createTable("ddw_fact_realtime_metric", "realtime_metric", fieldNames, fieldTypes);
            builder.put(table.getTableName().toUpperCase(Locale.getDefault()), table);

            List<String> fieldNames2 = Arrays.asList("source_code_group_id", "is_enabled");
            List<CIPFieldType> fieldTypes2 = Arrays.asList(CIPFieldType.STRING, CIPFieldType.BOOLEAN);
            CIPScannableTable table2 = (CIPScannableTable) createTable("ddw_dim_source_code_group", "src_code_grp", fieldNames2, fieldTypes2);
            builder.put(table2.getTableName().toUpperCase(Locale.getDefault()), table2);
        }
        return builder.build();
    }

    private Table createTable (String table, String name, List<String> fieldNames, List<CIPFieldType> fieldTypes) {

        return new CIPScannableTable(this, group, table, name, fieldNames, fieldTypes);
    }
}
