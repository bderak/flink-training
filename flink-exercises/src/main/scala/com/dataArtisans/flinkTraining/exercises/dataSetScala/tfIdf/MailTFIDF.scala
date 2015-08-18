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

package com.dataArtisans.flinkTraining.exercises.dataSetScala.tfIdf

import java.util.regex.Pattern

import com.dataArtisans.flinkTraining.dataSetPreparation.MBoxParser
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * Scala reference implementation for the "TF-IDF" exercise of the Flink training.
 * The task of the exercise is to compute the TF-IDF score for words in mails of the
 * Apache Flink developer mailing list archive.
 *
 * Required parameters:
 *   --input path-to-input-directory
 *
 */
object MailTFIDF {

   def main(args: Array[String]) {

     // parse parameters
     val params = ParameterTool.fromArgs(args)
     val input = params.getRequired("input")

     // set up the execution environment
     val env = ExecutionEnvironment.getExecutionEnvironment

     // read messageId and body field of the input data
     val mails = env.readCsvFile[(String, String)](
       input,
       lineDelimiter = MBoxParser.MAIL_RECORD_DELIM,
       fieldDelimiter = MBoxParser.MAIL_FIELD_DELIM,
       includedFields = Array(0,4)
     )

     // count mails in data set
     val mailCnt = mails.count()

     // compute term-frequency (TF)
     val tf = mails.flatMap {
       new FlatMapFunction[(String, String), (String, String, Int)] {
         // stop words to be filtered out
         val stopWords = List(
           "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
           "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
           "have", "as", "can")

         // pattern for recognizing acceptable 'word'
         val wordPattern: Pattern = Pattern.compile("(\\p{Alpha})+")

         def flatMap(mail: (String, String), out: Collector[(String, String, Int)]): Unit = {
           // extract email id
           val id = mail._1
           val output = mail._2.toLowerCase
             // split the body
             .split(Array(' ', '\t', '\n', '\r', '\f'))
             // filter out stop words and non-words
             .filter(w => !stopWords.contains(w) && wordPattern.matcher(w).matches())
             // count the number of occurrences of a word in each document
             .map(m => (m, 1)).groupBy(_._1).map {
             case (item, count) => (item, count.foldLeft(0)(_ + _._2))
           }
           // use the same mail id for each word in the body
           output.foreach(m => out.collect(id, m._1, m._2))
         }

       }
     }
     // compute document frequency (number of mails that contain a word at least once)
     // we can reuse the tf data set, since it already contains document <-> word association
     val df = mails.flatMap{
       new FlatMapFunction[(String, String), (String, Int)] {
         // stop words to be filtered out
         val stopWords = List(
           "the", "i", "a", "an", "at", "are", "am", "for", "and", "or", "is", "there", "it", "this",
           "that", "on", "was", "by", "of", "to", "in", "to", "message", "not", "be", "with", "you",
           "have", "as", "can")

         // pattern for recognizing acceptable 'word'
         val wordPattern: Pattern = Pattern.compile("(\\p{Alpha})+")

         def flatMap(mail: (String, String), out: Collector[(String, Int)]): Unit = {
           val output = mail._2.toLowerCase
             // split the body
             .split(Array(' ', '\t', '\n', '\r', '\f'))
             // filter out stop words and non-words
             .filter(w => !stopWords.contains(w) && wordPattern.matcher(w).matches())
             // count the number of occurrences of a word in each document
             .distinct
           // use the same mail id for each word in the body
           output.foreach(m => out.collect(m, 1))
         }

       }

     }
       // group by the words
       .groupBy(0)
       // count the number of documents in each group (df)
       .sum(1)
     
     // compute TF-IDF score from TF, DF, and total number of mails
     val tfidf = tf
       .join(df)
       // where "word" from tf
       .where(1)
       // is equal "word" from df
       .equalTo(0) {
       (l, r) => (l._1, l._2, l._3 * (mailCnt.toDouble / r._2))
     }
     
     // print the result
     tfidf
       .print()

   }
 }


