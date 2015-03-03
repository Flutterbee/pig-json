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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.bzip2r.Bzip2TextInputFormat;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.parser.ParserException;

/**
 * A general-purpose JSON loader. Based on the Pig 0.10 JsonLoader,
 * with extra features for robustness.
 *
 * If passed a schema, it will attempt to follow it as closely as possible. 
 * If there is an error parsing a field, it will be set to null with an error message 
 * logged, but the parser should keep going. 
 * Handles missing fields, extra fields, and/or out-of-order fields.
 *
 * Nested arrays are tricky, since Pig does not support directly nested bags;
 * you must wrap each nested bag in a tuple instead. For example, to load 
 * [[1, 2], [3, 4]], the schema must be arr: {t: (b: {t: (i: int)})}.
 * Note that since the values in the JSON array are anonymous, the names
 * of the fields in the schema do not matter.
 *
 * Sometimes, a nested array represents a tuple instead of a bag, 
 * ex. an array of coordinate pairs. JsonLoader allows arrays to be cast to tuple if
 * you guarantee that the schema matches, ex: coord_arr: {t: (coords: (lat: double, long: double))}
 * can load [[40.664167, -73.938611], [37.783333, -122.416667]] as {(40.664167, -73.938611), (37.783333, -122.416667)}
 *
 * If not passed a schema, it will load the entire document as a map,
 * detecting JSON nested objects and arrays and converting them into
 * tuples and bags respectively.
 *
 * LoadMetadata is implemented even though no metadata is being used, because
 * it provides the hook to the getSchema() method.
 *
 * Static methods are so that companion udfs
 * FromJsonInferSchema and FromJsonWithSchema can re-use code from here.
 */
public class JsonLoader extends LoadFunc implements LoadMetadata, LoadPushDown {
    private String udfContextSignature = null;

    private JsonFactory jsonFactory = null;
    private static TupleFactory tupleFactory = TupleFactory.getInstance();
    private static BagFactory bagFactory = BagFactory.getInstance();

    private final static Options validOptions_ = new Options();
    private final static CommandLineParser parser_ = new GnuParser();
    private final CommandLine configuredOptions_;

    private String loadLocation;    
    protected RecordReader reader = null;
    protected ResourceSchema schema = null;
    protected ResourceFieldSchema[] fields = null;

    private static final ResourceFieldSchema nestedMapField
        = new ResourceFieldSchema(new Schema.FieldSchema("obj", DataType.MAP));
    private static final ResourceFieldSchema nestedBytearrayField
        = new ResourceFieldSchema(new Schema.FieldSchema("b", DataType.BYTEARRAY));
    private static final ResourceFieldSchema emptyBagField
        = new ResourceFieldSchema(new Schema.FieldSchema(null, DataType.BAG));
    
    private boolean[] requiredFields = null;
    private boolean useDefaultSchema = false;
    private String inputFormatClassName = null;

    public static final String DEFAULT_MAP_NAME = "object";
    private static final String SCHEMA_SIGNATURE = "pig.jsonloader.schema";
    private static final String REQUIRED_FIELDS_SIGNATURE = "pig.jsonloader.required_fields";
    private static final Log log = LogFactory.getLog(JsonLoader.class);

    private static void populateValidOptions() {
        validOptions_.addOption("inputFormat", true, "The input format class name" +
            " used by this loader instance");
      }
    
    public JsonLoader() {
        this(DEFAULT_MAP_NAME + ":map[]", "");
        useDefaultSchema = true;
    }

    public JsonLoader(String schemaStr) {
        this(schemaStr, "");
    }
    public JsonLoader(String schemaStr, String optStr) {
        schema = new ResourceSchema(getEscapedSchemaFromString(schemaStr));
        fields = schema.getFields();
        
        populateValidOptions();
        String[] optsArr = optStr.split(" ");
        
        try {
          configuredOptions_ = parser_.parse(validOptions_, optsArr);
        } catch (org.apache.commons.cli.ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "[-inputFormat]", validOptions_ );
            throw new RuntimeException(e);
        }
        
        if (configuredOptions_.getOptionValue("inputFormat") != null) {
          this.inputFormatClassName = configuredOptions_.getOptionValue("inputFormat");
        }
    }
   

    public static Schema getEscapedSchemaFromString(String schemaStr) {
        schemaStr = schemaStr.replaceAll("[\\r\\n]", "");
        String[] fieldSchemaStrs = schemaStr.split(",");
        StringBuilder escapedSchemaBuilder = new StringBuilder();

        for (int i = 0; i < fieldSchemaStrs.length; i++) {
            escapedSchemaBuilder.append(escapeFieldSchemaStr(fieldSchemaStrs[i]));
            if (i != fieldSchemaStrs.length - 1)
                escapedSchemaBuilder.append(",");
        }

        try {
            return Utils.getSchemaFromString(escapedSchemaBuilder.toString());
        } catch (ParserException pe) {
            throw new IllegalArgumentException("Invalid schema format: " + pe.getMessage());
        }
    }

    // escape field names starting with underscores
    // ex. "__field: int" -> "u__field: int"
    // escape whitespace as directed, 
    // ex. "field\\ with\\ space: int" (Pig) -> "field\ with \ space: int" (fieldSchemaStr) -> "field_with_space: int" (escapedStr)
    private static String escapeFieldSchemaStr(String fieldSchemaStr) {
        String escaped = fieldSchemaStr.trim().replaceAll("\\\\\\s+", "_");
        if (escaped.charAt(0) == '_') {
            escaped = "underscore" + escaped;
        }
        return escaped;
    }

    private static String escapeFieldName(String fieldName) {
        String escaped = fieldName.trim().replaceAll("\\s+", "_");
        if (escaped.charAt(0) == '_') {
            escaped = "underscore" + escaped;
        }
        return escaped;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        loadLocation = location;
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        // if not manually set in options string
        if (inputFormatClassName == null) {
            if (loadLocation.endsWith(".bz2") || loadLocation.endsWith(".bz")) {
                inputFormatClassName = Bzip2TextInputFormat.class.getName();
            } else {
                inputFormatClassName = TextInputFormat.class.getName();
            }
        }

        try {
            return (FileInputFormat) PigContext.resolveClassName(inputFormatClassName).newInstance();
        } catch (InstantiationException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed creating input format " + inputFormatClassName, e);
        }
    }

    @Override
    public void setUDFContextSignature( String signature ) {
        udfContextSignature = signature;
    }

    public ResourceSchema getSchema(String location, Job job)
            throws IOException {

        if (schema != null) {
            // Send schema to backend
            // Schema should have been passed as an argument (-> constructor)
            // or provided in the default constructor

            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
            p.setProperty(SCHEMA_SIGNATURE, schema.toString());

            return schema;
        } else {
            // Should never get here
            throw new IllegalArgumentException(
                "No schema found: default schema was never created and no user-specified schema was found."
            );
        }
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split)
            throws IOException {

        // Save reader to use in getNext()
        this.reader = reader;

        UDFContext udfc = UDFContext.getUDFContext();
        Properties p = udfc.getUDFProperties(this.getClass(), new String[] { udfContextSignature });

        // Only want to try and recreate the schema/requiredFields if we don't already have it set.
        // When illustrating this method may be called multiple times without the correct
        // properties so we want to just use the values already set.
        if (schema == null) {
            // Get schema from front-end
            String schemaStr = p.getProperty(SCHEMA_SIGNATURE);
    
            if (schemaStr == null) {
                throw new IOException("Could not find schema in UDF context");
            }
            schema = new ResourceSchema(Utils.getSchemaFromString(schemaStr));
        }

        if (requiredFields == null) {
            String requiredFieldsStr = p.getProperty(REQUIRED_FIELDS_SIGNATURE);
            if (requiredFieldsStr != null) {
                requiredFields = (boolean[]) ObjectSerializer.deserialize(requiredFieldsStr);
            }
        }

        jsonFactory = new JsonFactory();
    }
    
    @Override
    public Tuple getNext() throws IOException {
        Text val = null;
        try {
            if (!reader.nextKeyValue()) return null;
            val = (Text) reader.getCurrentValue();
        } catch (Exception e) {
            throw new IOException(e);
        }

        // Create a parser specific for this input line.  
        // This may not be the most efficient approach.
        ByteArrayInputStream bais = new ByteArrayInputStream(val.getBytes());
        JsonParser p = jsonFactory.createJsonParser(bais);

        Tuple t;

        // schema provided
        if (!useDefaultSchema) {
            // Create a map of field names to ResourceFieldSchema's,
            // and create a map of field names to positions in the tuple.
            // These are used during parsing to handle extra, missing, and/or out-of-order
            // fields properly.

            Map<String, ResourceFieldSchema> schemaMap = new HashMap<String, ResourceFieldSchema>();
            Map<String, Integer> schemaPositionMap = new HashMap<String, Integer>();
            
            if (requiredFields != null) {
                int count = 0;
                for (int i = 0; i < fields.length; i++) {
                    if (requiredFields[i]) {
                        schemaMap.put(fields[i].getName(), fields[i]);
                        schemaPositionMap.put(fields[i].getName(), count);
                        count++;
                    }
                }
                t = tupleFactory.newTuple(count);
            } else {
                for (int i = 0; i < fields.length; i++) {
                    schemaMap.put(fields[i].getName(), fields[i]);
                    schemaPositionMap.put(fields[i].getName(), i);
                }
                t = tupleFactory.newTuple(fields.length);
            }

            try {
                p.nextToken(); // move to start of object
                parseObjectIntoTuple(val.toString(), p, schemaMap, schemaPositionMap, t);
            } catch (JsonParseException jpe) {
                // If the line doesn't parse as a valid JSON object, log an error and move on
                log.error("Error parsing record: " + val + ": " + jpe.toString());
            }
        } else {
            // schema not provided: load whole document as a map
            t = tupleFactory.newTuple(1);
            try {
                p.nextToken(); // move to start of object
                t.set(0, readField(val.toString(), p, schema.getFields()[0]));
            } catch (JsonParseException jpe) {
                log.error("Error parsing record: " + val + ": " + jpe.toString());
            }
        }

        p.close();
        return t;
    }

    /**
     * Given a JsonParser positioned at the beginning of an object,
     * parse that object and load its fields into the given tuple. 
     * Ignore fields that are not in the schemaMap and set any fields
     * that are missing or threw an exception while parsing to null.
     * 
     * The full text of the record is provided 1) to allow for more informative
     * error messages, and 2) to allow JSON objects/arrays to be type coerced
     * into bytearrays or chararrays properly if the schema requests it.
     *
     * Returns false if and END_ARRAY token is found. This indicates that the
     * calling method is iterating through the objects in an array/bag and
     * ought to stop.
     */
    public static boolean parseObjectIntoTuple(String text, JsonParser p, 
                                               Map<String, ResourceFieldSchema> schemaMap,
                                               Map<String, Integer> schemaPositionMap,
                                               Tuple t) 
                                               throws IOException {

        JsonToken tok = p.getCurrentToken();
        
        if (tok == JsonToken.END_ARRAY)
            return false;

        if (tok != JsonToken.START_OBJECT) {
            throw new JsonParseException("No start of object found", p.getCurrentLocation());
        }

        while((tok = p.nextToken()) != JsonToken.END_OBJECT) {
            if (tok != JsonToken.FIELD_NAME)
                throw new JsonParseException("No field name found", p.getCurrentLocation());

            String fieldName = escapeFieldName(p.getText());
            if (schemaMap.containsKey(fieldName)) {

                ResourceFieldSchema field = schemaMap.get(fieldName);
                int fieldNum = schemaPositionMap.get(fieldName).intValue();
                try {
                    p.nextToken(); // move to val
                    t.set(fieldNum, readField(text, p, field));
                } catch (Exception e) {
                    log.error("Exception when parsing field \"" + fieldName + "\" " +
                              "in record " + text + ": " + e.toString());
                    t.set(fieldNum, null);
                    p.skipChildren(); // if the error happened at a start object/array token
                                      // skip its children and move onto the next token
                                      // at this level
                }
            } else {
                // if the key is not in the schema
                // skip the value/object and move on

                tok = p.nextToken();
                p.skipChildren();
            }
        }

        return true;
    }

    /**
     * Read a single JSON value into an object (the parser should already be at the value). 
     * Will attempt type-coercion if requested.
     */
    public static Object readField(String text, 
                                   JsonParser p,
                                   ResourceFieldSchema field) 
                                   throws IOException, IllegalArgumentException {

        JsonToken tok = p.getCurrentToken();

        // used with complex types (map, tuple, bag)
        ResourceSchema nestedSchema;
        ResourceFieldSchema[] nestedFields;
        Map<String, ResourceFieldSchema> nestedSchemaMap;
        Map<String, Integer> nestedSchemaPositionMap;
        Tuple nestedTuple;

        switch (field.getType()) {
            case DataType.INTEGER:
                if (tok == JsonToken.VALUE_NULL) return null;
                if (!tok.isScalarValue()) {
                    throw new IllegalArgumentException(
                        "Bad integer field: expected scalar value but found non-scalar token " + p.getText());
                }
                if (tok == JsonToken.VALUE_STRING) return Integer.parseInt(p.getText());
                return p.getIntValue();

            case DataType.LONG:
                if (tok == JsonToken.VALUE_NULL) return null;
                if (!tok.isScalarValue()) {
                    throw new IllegalArgumentException(
                        "Bad long field: expected scalar value but found non-scalar token " + p.getText());
                }
                if (tok == JsonToken.VALUE_STRING) return Long.parseLong(p.getText());
                return p.getLongValue();

            case DataType.FLOAT:
                if (tok == JsonToken.VALUE_NULL) return null;
                if (!tok.isScalarValue()) {
                    throw new IllegalArgumentException(
                        "Bad float field: expected scalar value but found non-scalar token " + p.getText());
                }
                if (tok == JsonToken.VALUE_STRING) return Float.parseFloat(p.getText());
                return p.getFloatValue();

            case DataType.DOUBLE:
                if (tok == JsonToken.VALUE_NULL) return null;
                if (!tok.isScalarValue()) {
                    throw new IllegalArgumentException(
                        "Bad double field: expected scalar value but found non-scalar token " + p.getText());
                }
                if (tok == JsonToken.VALUE_STRING) return Double.parseDouble(p.getText());
                return p.getDoubleValue();

            case DataType.BYTEARRAY:
                String s = readFieldAsString(text, p, field);
                if (s != null) {
                    byte[] b = s.getBytes();
                    // Use the DBA constructor that copies the bytes so that we own the memory
                    // Above comment was in the Pig 0.10 JsonLoader - not sure what exactly it's saying
                    // but I kept the DataByteArray to be safe
                    return new DataByteArray(b, 0, b.length);
                } else
                    return null;

            case DataType.CHARARRAY:
                return readFieldAsString(text, p, field);

            case DataType.MAP:
                nestedSchema = field.getSchema();

                if (tok != JsonToken.START_OBJECT) {
                    // inferring schema in nested array
                    // should actually be a bag, not a map, so we redirect
                    if (tok == JsonToken.START_ARRAY) {
                        // empty bag field signals that we should keep inferring the nested schema
                        return readField(text, p, emptyBagField);
                    } else if (tok == JsonToken.VALUE_NULL) {
                        return null;
                    }
                    throw new IllegalArgumentException(
                        "Bad map field: expecting start of object, but found token " + p.getText());
                }

                Map<Object, Object> m = new HashMap<Object, Object>();
                while ((tok = p.nextToken()) != JsonToken.END_OBJECT) {
                    if (tok != JsonToken.FIELD_NAME) {
                        throw new IllegalArgumentException(
                            "Bad map field: expecting field name for child field, but found token " + p.getText());
                    }

                    String key = p.getText();
                    Object val = null;

                    if (nestedSchema != null) {
                        nestedFields = nestedSchema.getFields();
                        p.nextToken(); // move to val
                        val = readField(text, p, nestedFields[0]);
                    } else {
                        // If no internal schema for the map was specified, autodetect objects and arrays,
                        // and treat them as maps. Treat everything else as a bytearray.

                        tok = p.nextToken();
                        if (tok == JsonToken.START_OBJECT) {
                            val = readField(text, p, nestedMapField);
                        } else if (tok == JsonToken.START_ARRAY) {
                            // empty bag schema signals that we should keep inferring the nested schema
                            val = readField(text, p, emptyBagField);
                        } else {
                            // if the map is pointing to scalars, treat them all as bytearrays
                            val = readField(text, p, nestedBytearrayField);
                        }
                    }

                    m.put(key, val);
                }

                return m;

            case DataType.TUPLE:
                if (tok == JsonToken.START_OBJECT) {
                    nestedSchema = field.getSchema();
                    nestedFields = nestedSchema.getFields();

                    nestedSchemaMap = new HashMap<String, ResourceFieldSchema>();
                    nestedSchemaPositionMap = new HashMap<String, Integer>();
                    for (int i = 0; i < nestedFields.length; i++) {
                        nestedSchemaMap.put(nestedFields[i].getName(), nestedFields[i]);
                        nestedSchemaPositionMap.put(nestedFields[i].getName(), i);
                    }

                    nestedTuple = tupleFactory.newTuple(nestedFields.length);
                    try {
                        parseObjectIntoTuple(text, p, nestedSchemaMap, nestedSchemaPositionMap, nestedTuple);
                    } catch (JsonParseException jpe) {
                        throw new IllegalArgumentException(
                                "Error parsing nested tuple");
                    }
                    return nestedTuple;
                } else if (tok == JsonToken.VALUE_NULL) {
                    return null;
                } else {
                    throw new IllegalArgumentException(
                        "Bad tuple field: expecting start of object, but found token " + p.getText());
                }

            case DataType.BAG:
                if (tok != JsonToken.START_ARRAY) {
                    if (tok == JsonToken.VALUE_NULL) {
                        return null;
                    }
                    throw new IllegalArgumentException(
                        "Bad bag field: expecting start of array, but found token " + p.getText());
                }

                DataBag bag = bagFactory.newDefaultBag();
                nestedSchema = field.getSchema();

                boolean inferringSchema = false;

                // null field schema signals that we are coming from a map to an unspecified schema
                if (nestedSchema == null) {
                    inferringSchema = true;
                    tok = p.nextToken();

                    if (tok == JsonToken.START_ARRAY) {
                        // keep inferring schema
                        ResourceFieldSchema emptyBagSchema = new ResourceFieldSchema();
                        emptyBagSchema.setType(DataType.BAG);
                        nestedFields = new ResourceFieldSchema[] { emptyBagSchema };
                    } else if (tok == JsonToken.START_OBJECT) {
                        nestedFields = new ResourceFieldSchema[] { nestedMapField };
                    } else {
                        nestedFields = new ResourceFieldSchema[] { nestedBytearrayField };
                    }
                } else {
                    // if we do have a schema, drill down to the next level
                    // and get the tuple's schema

                    nestedFields = nestedSchema.getFields();
                    nestedSchema = nestedFields[0].getSchema();
                    nestedFields = nestedSchema.getFields();
                }

                byte firstNestedFieldType = nestedFields[0].getType();

                // If we are inferring the schema, we have already advanced tok
                // to lookahead and infer the schema
                if (!inferringSchema)
                    tok = p.nextToken();

                // nested array, ex, [[1, 2], [3, 4]]
                if (tok == JsonToken.START_ARRAY) {
                    // nested array -> bag of bags
                    if (nestedFields.length == 1 && nestedFields[0].getType() == DataType.BAG) {
                        while (p.getCurrentToken() != JsonToken.END_ARRAY) {
                            nestedTuple = tupleFactory.newTuple(1);
                            nestedTuple.set(0, readField(text, p, nestedFields[0]));
                            bag.add(nestedTuple);
                            p.nextToken();
                        }
                    } else {
                        // Casting nested array to tuple
                        while (true) {
                            nestedTuple = tupleFactory.newTuple(nestedFields.length);
                            for (int i = 0; i < nestedFields.length; i++) {
                                tok = p.nextToken();
                                if (!tok.isScalarValue())
                                    throw new IllegalArgumentException(
                                        "Invalid schema for array-to-tuple cast. Only flat arrays of scalars may be cast to tuples."
                                    );
                                nestedTuple.set(i, readField(text, p, nestedFields[i]));
                            }
                            bag.add(nestedTuple);
                            tok = p.nextToken(); // move to END_ARRAY
                            tok = p.nextToken(); // move to either next START_ARRAY
                                                 // or the END_ARRAY signalling the end of this nested level 
                            if (tok == JsonToken.END_ARRAY) {
                                break;
                            }
                        }
                    }
                } else if (tok == JsonToken.START_OBJECT) {
                    // array of objects, ex [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
                    if (nestedFields[0].getType() == DataType.MAP) {
                        // read as map
                        while (tok != JsonToken.END_ARRAY) {
                            nestedTuple = tupleFactory.newTuple(1);
                            nestedTuple.set(0, readField(text, p, nestedFields[0]));
                            bag.add(nestedTuple);
                            tok = p.nextToken();
                        }
                    } else {
                        // read as tuple

                        nestedSchemaMap = new HashMap<String, ResourceFieldSchema>();
                        nestedSchemaPositionMap = new HashMap<String, Integer>();
                        for (int i = 0; i < nestedFields.length; i++) {
                            nestedSchemaMap.put(nestedFields[i].getName(), nestedFields[i]);
                            nestedSchemaPositionMap.put(nestedFields[i].getName(), i);
                        }

                        boolean keepParsing = true;
                        while (keepParsing) {
                            nestedTuple = tupleFactory.newTuple(nestedFields.length);
                            // parseObjectIntoTuple will return false when we reach the END_ARRAY of this nested level
                            keepParsing = parseObjectIntoTuple(text, p, 
                                                               nestedSchemaMap, nestedSchemaPositionMap, 
                                                               nestedTuple);
                            if (keepParsing) {
                                bag.add(nestedTuple);
                                p.nextToken(); // move to next object start
                            }
                        }
                    }
                } else {
                    // flat array, ex, [1, 2, 3, 4]
                    // if we don't have a schema, treat everything like a bytearray
                    if (inferringSchema)
                        nestedFields = new ResourceFieldSchema[] { nestedBytearrayField };

                    while (tok != JsonToken.END_ARRAY) {
                        nestedTuple = tupleFactory.newTuple(1);
                        nestedTuple.set(0, readField(text, p, nestedFields[0]));
                        bag.add(nestedTuple);
                        tok = p.nextToken();
                    }
                }

                return bag;

            default:
                throw new IllegalArgumentException(
                    "Unknown type in input schema: " + field.getType());
        }
    }

    public static String readFieldAsString(String text,
                                           JsonParser p,
                                           ResourceFieldSchema field) 
                                           throws IOException {

        JsonToken tok = p.getCurrentToken();

        if (tok == JsonToken.VALUE_NULL) return null;

        if (tok.isScalarValue()) {
            return p.getText();
        } else {
            JsonToken start, end;
            int startPos, endPos, nesting;

            if (tok == JsonToken.START_OBJECT) {
                start = JsonToken.START_OBJECT;
                end = JsonToken.END_OBJECT;
            } else if (tok == JsonToken.START_ARRAY) {
                start = JsonToken.START_ARRAY;
                end = JsonToken.END_ARRAY;
            } else {
                // Should never happen
                throw new IllegalArgumentException(
                    "Bad bytearray/chararray field: expected scalar value or start object/array token, but found " + tok.asString());
            }

            // skip to the end of this object/array
            // The parser's reported column number depends on whitespace
            // and other weirdness I don't quite get, so the simplest way
            // that I could find to handle nested structures is to use
            // this "rewind" method

            p.skipChildren();

            endPos = p.getCurrentLocation().getColumnNr();
            startPos = endPos;
            if (end == JsonToken.END_OBJECT) {
                while (text.charAt(endPos) != '}' && endPos > 0) endPos--;
                nesting = 1;
                startPos = endPos;
                while (nesting > 0 && startPos > 0) {
                    startPos--;
                    if (text.charAt(startPos) == '}')
                        nesting++;
                    else if (text.charAt(startPos) == '{')
                        nesting--;
                }
            } else if (end == JsonToken.END_ARRAY) {
                while (text.charAt(endPos) != ']' && endPos > 0) endPos--;
                nesting = 1;
                startPos = endPos;
                while (nesting > 0 && startPos > 0) {
                    startPos--;
                    if (text.charAt(startPos) == ']')
                        nesting++;
                    else if (text.charAt(startPos) == '[')
                        nesting--;
                }
            }

            return text.substring(startPos, endPos + 1);
        }
    }

    @Override
    public RequiredFieldResponse pushProjection(RequiredFieldList requiredFieldList) throws FrontendException {
        if (requiredFieldList == null)
            return null;

        if (!useDefaultSchema && requiredFieldList.getFields() != null)
        {
            requiredFields = new boolean[fields.length];

            for (RequiredField f : requiredFieldList.getFields()) {
                requiredFields[f.getIndex()] = true;
            }

            UDFContext udfc = UDFContext.getUDFContext();
            Properties p = udfc.getUDFProperties(this.getClass(), new String[]{ udfContextSignature });
            try {
                p.setProperty(REQUIRED_FIELDS_SIGNATURE, ObjectSerializer.serialize(requiredFields));
            } catch (Exception e) {
                throw new RuntimeException("Cannot serialize requiredFields for pushProjection");
            }
        }

        return new RequiredFieldResponse(true);
    }

    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    public ResourceStatistics getStatistics(String location, Job job)
            throws IOException {
        // Not implemented
        return null;
    }

    public String[] getPartitionKeys(String location, Job job)
            throws IOException {
        // Not implemented
        return null;
    }

    public void setPartitionFilter(Expression partitionFilter)
            throws IOException {
        // Not implemented
    }
}