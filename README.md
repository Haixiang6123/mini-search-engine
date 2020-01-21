# CS221 Project - Mini Search Engine

This repo is for building a naive search engine based on Java. 
Junit is used for unit testing.
Maven is used as the main package manager.

## Get started

```bash
$ mvn clean install -DskipTests
```

## Components

* [x] Punctuation Tokenizer
* [x] Word Break Tokenizer (Support English, Japanese, Chinese)
* [x] Steammer (Using Lucene Lib)
* [x] Apply LSM method to do merge and flush
* [x] Compressor based on delta encoding and variable-length encoding
* [x] Page rank + TF-IDF rank
* [x] Phrase search

## Run tests

```bash
$ mvn test
```
