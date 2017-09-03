package app.atom

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import traits.Setup

//import utilities.ExtendedString._

//case class T()

trait Atom[T] {
  
  def process(sc: SparkContext, domainContext: DomainContext): Unit = {
    val inputRecords = parseTextFile(sc, domainContext.DATA_FILE, domainContext.HAS_HEADER, domainContext.FIELD_SEPARATOR)
    val sanitizedRecords = sanitizeRecords(inputRecords)
    val classifiedRecords = classifyRecords(sanitizedRecords)
    writeFile(classifiedRecords)
  }

  private def parseTextFile(sc: SparkContext, inputFileName: String, hasHeader: Boolean, fieldSeparator: String): RDD[Array[String]] = {

    val lines = sc.textFile(inputFileName)

    if (hasHeader) {
      val header = lines.first
      val records = lines.filter(line => line != header).map(line => line.split(fieldSeparator))
      return records
    } else {
      val records = lines.map(line => line.split(fieldSeparator))
      return records
    }

  }

  def sanitizeRecords(records: RDD[Array[String]]): RDD[Array[String]]

  def classifyRecords(records: RDD[Array[String]]): RDD[T]

  def writeFile(records: RDD[T]): Unit 
}