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

public class TestFromJsonInferSchema {
    private static final String dataDir = "build/test/tmpdata/";
    private static final String scalarInput = "TestFromJsonInferSchema_scalar_input";
    private static final String complexInput = "TestFromJsonInferSchema_complex_input";
    private static final String nestedArrayInput = "TestFromJsonInferSchema_nested_array_input";

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
        } catch (IOException e) {};
    }

    @After
    public void cleanup() throws IOException {
        Util.deleteDirectory(new File(dataDir));
        pig.shutdown();
    }

    @Test
    public void scalarTypesSchemaless() throws IOException, ParseException {
        String input = scalarInput;

        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate com.mortardata.pig.FromJsonInferSchema(text);");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "([f#2.718,l#10,d#3.1415,b#17,c#aardvark,i#1])",
            "([f#1.234,l#100,d#3.3333,b#,c#17.0,i#2])"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void complexTypesSchemaless() throws IOException, ParseException {
        String input = complexInput;

        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate com.mortardata.pig.FromJsonInferSchema(text);");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "([map#{b=2, a=1},nested_bag#{([b#{([c#2,d#3]),([c#4,d#5])},a#1])},nested_map#{a={b=1, c=2}},tuple#{b=2, a=1},bag#{([b#2,a#1]),([b#4,a#3])},nested_tuple#{b={c=2, d=3}, a=1}])",
            "([map#{b=4, a=3},nested_bag#{([b#{([c#7,d#8]),([c#9,d#0])},a#6])},nested_map#{a={b=3, c=4}},tuple#{b=4, a=3},bag#{([b#6,a#5]),([b#8,a#7])},nested_tuple#{b={c=5, d=6}, a=4}])"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }

    @Test
    public void nestedArraysSchemaless() throws IOException, ParseException {
        String input = nestedArrayInput;

        pig.registerQuery("data = load '" + dataDir + input + "' using TextLoader() as (text: chararray);");
        pig.registerQuery("json = foreach data generate com.mortardata.pig.FromJsonInferSchema(text);");

        Iterator<Tuple> json = pig.openIterator("json");
        String[] expected = {
            "([nested_arr_2#{({(1),(2)}),({(3),(4)})},nested_arr#{({(1),(2)}),({(3),(4)})},arr#{(1),(2),(3),(4)},very_nested_arr#{({({(1),(2)}),({(3),(4)})}),({({(5),(6)}),({(7),(6)})})},i#9])"
        };

        Assert.assertEquals(StringUtils.join(expected, "\n"), StringUtils.join(json, "\n"));
    }
}