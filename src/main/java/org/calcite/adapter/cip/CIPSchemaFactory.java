package org.calcite.adapter.cip;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

public class CIPSchemaFactory implements SchemaFactory {

    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> map) {

        CIPSchema schema = new CIPSchema((String)map.get("group"));
        return schema;
    }
}
