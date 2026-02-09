# Big Data Analysis Technologies - Scala

This repository contains the implementation of a project focuses on processing large datasets using **Apache Spark** with **Scala**.

## Project Overview

The assignment is divided into two sub-tasks, demonstrating both the **RDD API** and **Spark SQL/DataFrames**:

### 1. Average Word Length Analysis (RDD)
* **Goal:** Calculate the average length of words for each starting letter in a text corpus.
* **Input Data:** `SherlockHolmes.txt`.
* **Implementation Logic:**
  * Uses the **RDD API**.
  * Cleans the text (converts to lowercase, removes punctuation) and filters for valid words starting with a letter.
  * Maps words to key-value pairs `(starting_letter, (length, 1))` and aggregates them using `reduceByKey`.
  * Computes the average and sorts the results in descending order.
* **File:** `Subtask1.scala`

### 2. Tweets Analysis (DataFrames)
* **Goal:** Analyze a dataset of tweets to extract sentiment insights and complaint reasons.
* **Input Data:** `tweets.csv`.
* **Implementation Logic:**
  * Uses **Spark DataFrames** and loads data with `inferSchema`.
  * **Q1: Top 5 Words per Sentiment:** Cleans text, explodes sentences into words, and uses Window functions (`row_number`) to find the most frequent words for "positive", "negative", and "neutral" sentiments.
  * **Q2: Main Complaint Reason:** Filters tweets with high confidence (`> 0.5`), groups by airline, and identifies the most common complaint reason for each airline.
* **File:** `Subtask2.scala`

---

## How to Run

### Prerequisites
* Apache Spark
* Scala
* IntelliJ IDEA (optional, for development)

### Execution Steps

1. **Setup Data:**
   Ensure the input files (`SherlockHolmes.txt` and `tweets.csv`) are placed in the `src/main/resources/` directory.

2. **Run Subtask 1:**
   Execute the `Subtask1` object. It will print the average word length per letter in the console (e.g., `c 7.09`).

3. **Run Subtask 2:**
   Execute the `Subtask2` object. It will display:
   * A table of the top 5 words for each sentiment category.
   * A table showing the main complaint reason for each airline.

---

## Sample Results

### Subtask 1: Average Word Length (Top 3)
1. **c**: 7.09
2. **e**: 7.03
3. **q**: 6.96

### Subtask 2: Insights
* **Top Negative Words:** Common words include "to", "the", "a", "and", "on".
* **Main Complaint Reasons:**
  * **American, United, Southwest, US Airways, Virgin America:** "Customer Service Issue".
  * **Delta:** "Late Flight".
