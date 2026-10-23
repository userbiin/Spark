# Project #: Project Name

* Author: Subin Min
* Class: CS435-535 Section 1
* Semester: Fall 2026

## Overview
This project improves the inverted index MapReduce example using Spark. The original version emits, for each word, every filename in which the word appears — including duplicates, so a word occur many copies of that filename. This version instead counts the occurrences of each word per file and emits each filename once, prefixed by its count. The filenames for a given word are ordered by count in descending order, and filenames with equal counts are ordered ascending by filename. The program is written in Python and runs under Apache Spark.

## Reflection

Write a one or two paragraph reflection describing your experience with this project.  Talk about
what worked well and what was challenging.  Did you run into an issue that took some time to
figure out?  Tell us about it. What did you enjoy, what was less desirable? Feel free to add
other items (within the two paragraph limit).

## Compiling and Using

Instruct the user how build, package and use your code. Does your program require user input? If
so, what does your user need to know about it to use it as quickly as possible?

## Testing

Describe a concrete plan on how you tested for correctness. For example, did you use a small
test file that you could hand check. Did you run Python scripts or write a script to generate
results directly for a small data set  and compare with the cluster output?

## Results 

This section presents timing and other results of the experiments that you were asked to perform
as part of the project.

## Sources used
- Apache Spark Exmaples — https://spark.apache.org/examples.html
- Spark API Reference — https://spark.apache.org/docs/latest/api/python/reference/index.html
- Claude (Anthropic) — used to translate the project specification, to translate my Korean draft of this README into English, to debug the code and to format the results into Markdown tables.
