package app.usb

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.util.Date
import java.lang.Double
import org.apache.commons.lang3.StringUtils

import traits.Setup
import app.atom.Atom

case class USBCheckingStandard(date: String, transaction: String, name: String, memo: String, expense: Double, income: Double, transfer: Double, deposit: Double)

object USB extends Atom[USBCheckingStandard] with USBContext {

  def process(sc: SparkContext) {
    process(sc, this)
  }

  override def sanitizeRecords(records: RDD[Array[String]]): RDD[Array[String]] = {
    val usbRecords = records.map { record =>
      val usbDate = StringUtils.remove(record(0), SPECIAL_CHAR_DOUBLE_QUOTE)
      val usbTransaction = StringUtils.remove(record(1), SPECIAL_CHAR_DOUBLE_QUOTE)
      val usbName = StringUtils.remove(record(2), SPECIAL_CHAR_DOUBLE_QUOTE)
      val usbMemo = StringUtils.remove(record(3), SPECIAL_CHAR_DOUBLE_QUOTE).replace("Download from usbank.com. ", "")
      val amount = StringUtils.remove(record(4), SPECIAL_CHAR_DOUBLE_QUOTE)
      val usbRecord = Array(usbDate, usbTransaction, usbName, usbMemo, amount)
      usbRecord
    }
    return usbRecords
  }

  override def classifyRecords(records: RDD[Array[String]]): RDD[USBCheckingStandard] = {
    val usbStdRecords = records.map { record =>
/*      val usbStdDate = {
        val expectedPattern = INPUT_DATE_FORMAT
        SimpleDateFormatStringToDate(expectedPattern, record(0))
      }*/
      val usbStdDate = record(0)
      val usbStdTransaction = record(1)
      val usbStdName = record(2)
      val usbStdMemo = record(3)
      val category = determine(usbStdTransaction, usbStdName)
      var expense: Double = 0.0
      var income: Double = 0.0
      var transfer: Double = 0.0
      var deposit: Double = 0.0
      //import java.lang.Double
      val amount = Double.parseDouble(record(4))
      category match {
        case CATEGORY_EXPENSE => expense = amount
        case CATEGORY_INCOME => income = amount
        case CATEGORY_TRANSFER => transfer = amount
        case CATEGORY_DEPOSIT => deposit = amount
      }
      val usbStdRecord = USBCheckingStandard(usbStdDate, usbStdTransaction, usbStdName, usbStdMemo, expense, income, transfer, deposit)
      usbStdRecord
    }
    return usbStdRecords
  }

  override def writeFile(usbStdRecords: RDD[USBCheckingStandard]): Unit = {
    val dirSummary = OUTPUT_DIR + "/usb-summary"
    val output = usbStdRecords.map { usbStdRecord => usbStdRecord.productIterator.mkString(FIELD_SEPARATOR) }
    output.coalesce(1, false).saveAsTextFile(dirSummary)
  }

  private def determine(usbStdTransaction: String, usbStdName: String): String = {

    val category: String = usbStdTransaction.toUpperCase() match {
      case INPUT_TRANSACTION_DEBIT => {
        val category = determineExpenseOrTransfer(usbStdName)
        category
      }
      case INPUT_TRANSACTION_CREDIT => {
        val category = determineIncomeOrDeposit(usbStdName)
        category
      }
      case _ => {
        CATEGORY_EXPENSE
      }
    }

    return category
  }

  private def determineExpenseOrTransfer(usbStdName: String): String = {
    var isTransfer: Boolean = false
    TRANSFER_DESTINATION.keys.foreach(destination => {
      if (usbStdName.toUpperCase().contains(destination)) isTransfer = true
    })

    if (isTransfer) CATEGORY_TRANSFER
    else CATEGORY_EXPENSE
  }

  private def determineIncomeOrDeposit(usbStdName: String): String = {
    var isIncome: Boolean = false
    INCOME_SOURCE.keys.foreach(source => {
      if (usbStdName.toUpperCase().contains(source)) isIncome = true
    })

    if (isIncome) CATEGORY_INCOME
    else CATEGORY_DEPOSIT
  }

  def SimpleDateFormatStringToDate(expectedPattern: String, input: String): Date = {
    import java.text.ParseException
    import java.text.SimpleDateFormat
    val formatter: SimpleDateFormat = new SimpleDateFormat(expectedPattern)
    formatter.parse(input)
  }
  
  def SimpleDateFormatDateToString(expectedPattern: String, input: Date): String = {
    import java.text.ParseException
    import java.text.SimpleDateFormat
    val formatter: SimpleDateFormat = new SimpleDateFormat(expectedPattern)
    formatter.format(input)
  }  

}