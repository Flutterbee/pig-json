# pig-json

[![Build Status](https://travis-ci.org/mortardata/pig-json.svg?branch=master)](https://travis-ci.org/mortardata/pig-json)

Mortar's JSON tools for Apache Pig.

## What's Inside

Included in this repo are:

* **JsonLoader**: A Pig load function for arbitrary JSON files (not just ones produced by JsonStorage)
* **FromJsonInferSchema**: A Pig UDF that wraps JsonLoader to transform JSON in a chararray (string) field into a Map. Infers the schema of the output.
* **FromJsonWithSchema**: Like FromJsonInferSchema, but you provide the schema.

These tools work for Apache Pig 0.12.1. They haven't been ported yet to work with later versions of Pig.

The benefit of using pig-json over Pig's built-in JsonStorage is that pig-json reads arbitrary JSON data without a metadata file, where the Pig built-in JsonStorage can only read data that it created with a metadata file. pig-json is also fairly robust against messy data.

## How to Use

To use these tools, package up a JAR file with maven:

```bash
# creates a jar file in target/pig-json-1.0.jar
mvn package -DskipTests=true
```

Then, register this jar in your Pigscript and use the load functions or UDFs:

```pig
register '/path/to/my/pig-json-1.0.jar';

-- Load up some json
tweets =  LOAD '/path/to/some/tweets.json' 
         USING com.mortardata.pig.JsonLoader(
                   'coordinates:map[], created_at:chararray, current_user_retweet:map[], entities:map[], favorited:chararray, id_str:chararray, in_reply_to_screen_name:chararray, in_reply_to_status_id_str:chararray, place:map[], possibly_sensitive:chararray, retweet_count:int, source:chararray, text:chararray, truncated:chararray, user:map[], withheld_copyright:chararray, withheld_in_countries:{t:(country:chararray)}, withheld_scope:chararray');
```

For lots more details on the load functions and UDFs, see Mortar's [JSON help page](https://help.mortardata.com/integrations/amazon_s3/json).

**IMPORTANT**: when using this library, use **`com.mortardata.pig.JsonLoader`** instead of `org.apache.pig.piggybank.storage.JsonLoader`.

