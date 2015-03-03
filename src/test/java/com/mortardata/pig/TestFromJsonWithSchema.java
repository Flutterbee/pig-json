package com.mortardata.pig;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;

import org.apache.pig.ExecType;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.pig.PigServer;
import org.apache.pig.tools.parameters.ParseException;
import org.apache.pig.test.Util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFromJsonWithSchema {
    private static final String dataDir = "build/test/tmpdata/";
    private static final String scalarInput = "TestFromJsonWithSchema_scalar_input";
    private static final String complexInput = "TestFromJsonWithSchema_complex_input";
    private static final String nestedArrayInput = "TestFromJsonWithSchema_nested_array_input";
    private static final String unusualFieldNameInput = "TestFromJsonWithSchema_unusual_field_name_input";

    static PigServer pig;

    @Before
    public void setup() throws IOException {
        pig = new PigServer(ExecType.LOCAL);

        Util.deleteDirectory(new File(dataDir));
        try {
            pig.mkdirs(dataDir);

            Util.createLocalInputFile(dataDir + scalarInput,
                new String[] {
                    "{ \"i\": 1, \"l\": 10, \"f\": 2.718, \"d\": 3.1415, \"b\": \"17\", \"c\": \"aardvark\" }",
                    "{ \"i\": 2, \"l\": 100, \"f\": 1.234, \"d\": 3.3333, \"b\": null, \"c\": \"17.0\" }"
            });

            Util.createLocalInputFile(dataDir + complexInput,
                new String[] {
                    "{ \"tuple\": { \"a\": 1, \"b\": 2 }, \"nested_tuple\": { \"a\": 1, \"b\": { \"c\": 2, \"d\": 3 } }, \"bag\": [{ \"a\": 1, \"b\": 2 }, { \"a\": 3, \"b\": 4 }], \"nested_bag\": [{\"a\": 1, \"b\": [{ \"c\": 2, \"d\": 3 }, { \"c\": 4, \"d\": 5 }]}], \"map\": { \"a\": 1, \"b\": 2 }, \"nested_map\": { \"a\": { \"b\": 1, \"c\": 2 } } }",
                    "{ \"tuple\": { \"a\": 3, \"b\": 4 }, \"nested_tuple\": { \"a\": 4, \"b\": { \"c\": 5, \"d\": 6 } }, \"bag\": [{ \"a\": 5, \"b\": 6 }, { \"a\": 7, \"b\": 8 }], \"nested_bag\": [{\"a\": 6, \"b\": [{ \"c\": 7, \"d\": 8 }, { \"c\": 9, \"d\": 0 }]}], \"map\": { \"a\": 3, \"b\": 4 }, \"nested_map\": { \"a\": { \"b\": 3, \"c\": 4 } } }"
            });

            Util.createLocalInputFile(dataDir + nestedArrayInput,
                new String[] {
                    "{ \"arr\": [1, 2, 3, 4], \"nested_arr\": [[1, 2], [3, 4]], \"nested_arr_2\": [[1, 2], [3, 4]], \"very_nested_arr\": [[[1, 2], [3, 4]], [[5, 6], [7, 6]]], \"i\": 9 }"
            });

            Util.createLocalInputFile(dataDir + unusualFieldNameInput,
                new String[] {
                    "{\"f_1\": 1, \"__f2\": 2, \"f 3\": 3}",
                    "{\"f_1\": 4, \"__f2\": 5, \"f 3\": 6}"
            });
        } catch (IOException e) {};
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }

    @Test
    public void scalarTypes() throws IOException, ParseException {
        String input = scalarInput;
        String schema = "i: int, l: long, f: float, d: double, b: bytearray, c: chararray";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "(1,10,2.718,3.1415,17,aardvark)",
            "(2,100,1.234,3.3333,,17.0)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void complexTypes() throws IOException, ParseException {
        String input = complexInput;
        String schema = "" +
            "tuple: (a: int, b: int), nested_tuple: (a: int, b: (c: int, d: int)), " +
            "bag: {t: (a: int, b: int)}, nested_bag: {t: (a: int, b: {tt: (c: int, d: int)})}, " +
            "map: [int], nested_map: [map[int]]";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "((1,2),(1,(2,3)),{(1,2),(3,4)},{(1,{(2,3),(4,5)})},[b#2,a#1],[a#{b=1, c=2}])",
            "((3,4),(4,(5,6)),{(5,6),(7,8)},{(6,{(7,8),(9,0)})},[b#4,a#3],[a#{b=3, c=4}])"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void nestedArrays() throws IOException, ParseException {
        String input = nestedArrayInput;
        String schema = "" +
            "arr: {t: (i: int)}, " +                                            // flat array [1, 2, 3, 4] -> {(int)}
            "nested_arr: {t: (b: {t: (i: int)})}, " +                           // nested array [[1, 2], [3, 4]] -> {({(int)})}
            "nested_arr_2: {t: (t: (a: int, b: int))}, " +                      // cast nested array [[1, 2], [3, 4]] to tuples: {((int, int))}
            "very_nested_arr: {t: (b: {t: (b: {t: (a: int, b: int)})})}, " +    // deeply nested array [[[1,2],[3,4]],[[5,6],[7,8]]] -> {({({(int)})})}
            "i: int";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "({(1),(2),(3),(4)},{({(1),(2)}),({(3),(4)})},{(1,2),(3,4)},{({({(1),(2)}),({(3),(4)})}),({({(5),(6)}),({(7),(6)})})},9)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void scalarTypeCoercion() throws IOException, ParseException {
        String input = scalarInput;
        String schema = "i: float, l: double, f: int, d: (a: int, b: int), b: int, c: float";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "(1.0,10.0,2,,17,)",
            "(2.0,100.0,1,,,17.0)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void complexTypeCoercion() throws IOException, ParseException {
        String input = complexInput;
        String schema = "" +
            "tuple: bytearray, nested_tuple: map[], " +
            "bag: chararray, nested_bag: int";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "({ \"a\": 1, \"b\": 2 },[b#{c=2, d=3},a#1],[{ \"a\": 1, \"b\": 2 }, { \"a\": 3, \"b\": 4 }],)",
            "({ \"a\": 3, \"b\": 4 },[b#{c=5, d=6},a#4],[{ \"a\": 5, \"b\": 6 }, { \"a\": 7, \"b\": 8 }],)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }


    // Missing fields, extra fields, out-of-order fields
    @Test
    public void inexactSchema() throws IOException, ParseException {
        String input = "TestFromJsonWithSchema_inexact_schema";
        String schema = "a: int, b: int, c: int, d: (i: int, j: int), e: int";

        Util.createLocalInputFile(dataDir + input,
            new String[] {
                "{ \"a\": 1, \"b\": 2, \"c\": 3, \"d\": { \"i\": 4, \"j\": 5 }, \"e\": 6 }",
                "{ \"a\": 1, \"c\": 3 }",
                "{ \"a\": 1, \"b\": 2, \"d\": { \"i\": 4, \"j\": 5 }, \"e\": 6 }",
                "{ \"a\": 1, \"extra\": \"extra\", \"b\": 2, \"d\": { \"i\": 4, \"j\": 5 } }",
                "{ \"b\": 2, \"a\": 1, \"e\": 6, \"d\": { \"i\": 4, \"j\": 5}, \"c\": 3 }"
        });

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "(1,2,3,(4,5),6)",
            "(1,,3,,)",
            "(1,2,,(4,5),6)",
            "(1,2,,(4,5),)",
            "(1,2,3,(4,5),6)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }
    
    @Test
    public void malformedDocuments() throws IOException, ParseException {
        String input = "TestFromJsonWithSchema_inexact_schema_malformed_documents";
        String schema = "a: int, b: int";

        Util.createLocalInputFile(dataDir + input,
            new String[] {
                "{ \"a\": 1, \"b\": 2 }",
                "{ \"a\": 1, \"b\": \"2abc\" }",
                "{ \"a\": 1, \"b\": abc2 }",
                "{ \"a\": 1, \"b\": false }",
                "{ \"a\": 1, \"b\": true }",
                "{ \"a\": 1, b: }",
                "{ \"a\": 1, ",
                "\"b\": 2 }"
        });

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "(1,2)",
            "(1,)",
            "(1,)",
            "(1,)",
            "(1,)",
            "(1,)",
            "(1,)",
            "(,)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void newlinesInSchema() throws IOException, ParseException {
        String input = scalarInput;
        String schema = "\n i: int, \n\n l: long, \r\n f: float, \n\n d: double, \r\n b: bytearray, \n\n c: chararray";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "(1,10,2.718,3.1415,17,aardvark)",
            "(2,100,1.234,3.3333,,17.0)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void nestedObjectPruning() throws IOException, ParseException {
        String input = complexInput;
        String schema = "tuple: (a: int, b: int), nested_tuple: (a: int, b: (c: int, d: int))";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");
        pig.registerQuery("projected = foreach json GENERATE $0.a, $1.b;");

        Iterator<Tuple> projected = pig.openIterator("projected");
        String[] expected = {
            "(1,(2,3))",
            "(3,(5,6))"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(projected, "\n"));
    }

    @Test
    public void nestedObjectPartialLoad() throws IOException, ParseException {
        String input = complexInput;
        String schema = "tuple: (a: int), nested_tuple: (b: (c: int))";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "((1),((2)))",
            "((3),((5)))"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void handleUnusualFieldNames() throws IOException, ParseException {
        String input = unusualFieldNameInput;
        String schema = "f_1: int, __f2: int, f\\\\ 3: int";

        pig.registerQuery("DEFINE FromJson com.mortardata.pig.FromJsonWithSchema('" + schema + "');");
        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate FLATTEN(FromJson(text));");
        pig.registerQuery("projected = foreach json generate underscore__f2, f_3;");

        Iterator<Tuple> projected = pig.openIterator("projected");
        String[] expected = {
            "(2,3)",
            "(5,6)"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(projected, "\n"));
    }
}