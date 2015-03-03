/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mortardata.pig;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigWarning;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.parser.ParserException;

public class FromJsonWithSchema extends EvalFunc<Tuple> {

    private static final Log log = LogFactory.getLog(FromJsonWithSchema.class);

    private JsonFactory jsonFactory;
    private TupleFactory tupleFactory;

    private Schema logicalSchema;
    private ResourceSchema resourceSchema;
    private ResourceFieldSchema[] resourceSchemaFields;

    public FromJsonWithSchema(String schemaStr) {
        jsonFactory = new JsonFactory();
        tupleFactory = TupleFactory.getInstance();

        logicalSchema = JsonLoader.getEscapedSchemaFromString(schemaStr);
        resourceSchema = new ResourceSchema(logicalSchema);
        resourceSchemaFields = resourceSchema.getFields();
    }

    @Override
    public Tuple exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        try {
            String jsonStr = (String) input.get(0);

            JsonParser p = jsonFactory.createJsonParser(jsonStr);
            Tuple t = tupleFactory.newTuple(resourceSchemaFields.length);

            // Create a map of field names to ResourceFieldSchema's,
            // and create a map of field names to positions in the tuple.
            // These are used during parsing to handle extra, missing, and/or out-of-order
            // fields properly.
            Map<String, ResourceFieldSchema> schemaMap = new HashMap<String, ResourceFieldSchema>();
            Map<String, Integer> schemaPositionMap = new HashMap<String, Integer>();
            for (int i = 0; i < resourceSchemaFields.length; i++) {
                schemaMap.put(resourceSchemaFields[i].getName(), resourceSchemaFields[i]);
                schemaPositionMap.put(resourceSchemaFields[i].getName(), i);
            }

            try {
                p.nextToken(); // move to start of object
                JsonLoader.parseObjectIntoTuple(jsonStr, p, schemaMap, schemaPositionMap, t);
            } catch (JsonParseException jpe) {
                // If the line doesn't parse as a valid JSON object, log an error and move on
                log.error("Error parsing input: " + jsonStr + ": " + jpe.toString());
            }

            p.close();
            return t;
        } catch (ExecException e) {
            warn("Error reading input: " + e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(
            new Schema.FieldSchema("object", logicalSchema)
        );
    }
}
