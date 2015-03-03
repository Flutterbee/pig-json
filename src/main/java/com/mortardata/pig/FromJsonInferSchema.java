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
import org.apache.pig.impl.util.Utils;

public class FromJsonInferSchema extends EvalFunc<Map> {

    private static final Log log = LogFactory.getLog(FromJsonInferSchema.class);

    private JsonFactory jsonFactory;
    private TupleFactory tupleFactory;

    public FromJsonInferSchema() {
        jsonFactory = new JsonFactory();
        tupleFactory = TupleFactory.getInstance();
    }

    @Override
    public Map exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }
        try {
            String jsonStr = (String) input.get(0);
            String schemaStr = "object: map[]";

            ResourceSchema schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));
            ResourceFieldSchema[] fields = schema.getFields();

            JsonParser p = jsonFactory.createJsonParser(jsonStr);

            Tuple t = tupleFactory.newTuple(1);
            try {
                p.nextToken(); // move to start of object
                t.set(0, JsonLoader.readField(jsonStr, p, fields[0]));
            } catch (JsonParseException jpe) {
                log.error("Error parsing input: " + jsonStr + ": " + jpe.toString());
            }

            p.close();
            return (Map) t.get(0);
        } catch (ExecException e) {
            warn("Error reading input: " + e.getMessage(), PigWarning.UDF_WARNING_1);
            return null;
        }
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(
            new Schema.FieldSchema("object", DataType.MAP)
        );
    }
}
