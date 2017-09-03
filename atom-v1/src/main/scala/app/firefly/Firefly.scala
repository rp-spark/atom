package app.firefly

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import java.lang.Double
import org.apache.commons.lang3.StringUtils

import traits.Setup
import app.atom.Atom

case class FireflyCheckingStandard(fflyDate: String, fflyDescription: String, fflyMemo: String, deposit: Double, creditCard: Double, utility: Double, home: Double, generalExpense: Double)

object Firefly extends Atom[FireflyCheckingStandard] with FireflyContext {

  def process(sc: SparkContext) {
    process(sc, this)
  }

  override def sanitizeRecords(records: RDD[Array[String]]): RDD[Array[String]] = {
    val fflyRecords = records.map { record =>
      val fflyTransactionNumber = StringUtils.remove(record(0), SPECIAL_CHAR_DOUBLE_QUOTE)
      val fflyDate = StringUtils.remove(record(1), SPECIAL_CHAR_DOUBLE_QUOTE)
      val fflyDescription = StringUtils.remove(record(2), SPECIAL_CHAR_DOUBLE_QUOTE)
      val fflyMemo = StringUtils.remove(record(3), SPECIAL_CHAR_DOUBLE_QUOTE)
      val fflyAmountDebit = StringUtils.remove(record(4), SPECIAL_CHAR_DOUBLE_QUOTE)
      val fflyAmountCredit = StringUtils.remove(record(5), SPECIAL_CHAR_DOUBLE_QUOTE)
      val fflyBalance = StringUtils.remove(record(6), SPECIAL_CHAR_DOUBLE_QUOTE)

      val fflyRecord = Array(fflyTransactionNumber, fflyDate, fflyDescription, fflyMemo, fflyAmountDebit, fflyAmountCredit, fflyBalance)
      fflyRecord
    }
    return fflyRecords
  }

  override def classifyRecords(records: RDD[Array[String]]): RDD[FireflyCheckingStandard] = {
    val fireflyStdRecords = records.map { record =>

      val fflyTransactionNumber = record(0)
      val fflyDate = record(1)
      val fflyDescription = record(2)
      val fflyMemo = record(3)
      val fflyAmountDebit = record(4)
      val fflyAmountCredit = record(5)
      val fflyBalance = record(6)

      val category = determineTransactionCategory(fflyDescription, fflyMemo)
      var deposit: Double = 0.0
      var creditCard: Double = 0.0
      var utility: Double = 0.0
      var home: Double = 0.0
      var generalExpense: Double = 0.0

      category match {
        case CATEGORY_DEPOSIT => deposit = Double.parseDouble(fflyAmountCredit)
        case CATEGORY_CREDIT_CARD => creditCard = Double.parseDouble(fflyAmountDebit)
        case CATEGORY_UTILITY => utility = Double.parseDouble(fflyAmountDebit)
        case CATEGORY_HOME => home = Double.parseDouble(fflyAmountDebit)
        case CATEGORY_GENERAL_EXPENSE => generalExpense = Double.parseDouble(fflyAmountDebit)
      }
      val fflyStdRecord = FireflyCheckingStandard(fflyDate, fflyDescription, fflyMemo, deposit, creditCard, utility, home, generalExpense)
      fflyStdRecord
    }
    return fireflyStdRecords
  }

  override def writeFile(fireflyStdRecords: RDD[FireflyCheckingStandard]): Unit = {
    val dirSummary = OUTPUT_DIR + "/firefly-summary"
    val output = fireflyStdRecords.map { fireflyStdRecord => fireflyStdRecord.productIterator.mkString(FIELD_SEPARATOR) }
    output.coalesce(1, false).saveAsTextFile(dirSummary)
  }

  private def determineTransactionCategory(description: String, memo: String): String = {

    /*
     *  CATEGORY_DEPOSIT: COLUMN_2_DIVIDEND or COLUMN_2_DEBIT_CARD_REVERSAL or (COLUMN_2_ELECTRONIC_DEPOSIT and (US FEDERAL CU or COLUMN_3_US_FCU or COLUMN_3_CAPITAL_ONE_NA))
     *  CATEGORY_CREDIT_CARD: COLUMN_2_ELECTRONIC_WITHDRAWAL and (COLUMN_3_CAPITAL_ONE_CARD or COLUMN_3_SAMS_CLUB_MC or TARGET CARD or KOHLS ONLINE
     * 	CATEGORY_UTILITY: (COLUMN_2_ELECTRONIC_WITHDRAWAL and (COLUMN_3_CTRPT_ENGY or COLUMN_3_SHAKOPEE_PUBLIC_ONLINE or COLUMN_3_COMCAST_ONLINE)) or COLUMN_2_SHARE_DRAFT
     * 	CATEGORY_HOME: (COLUMN_2_ELECTRONIC_WITHDRAWAL and (COLUMN_3_AFFINITY_PLUS or COLUMN_3_CORNERSTONE_MGMTONLINE or COLUMN_3_SCOTT_COUNTY_ONLINE) or (COLUMN_2_DEBIT_CARD_DEBIT and COLUMN_3_THE_HOME_DEP)
     * 
     */
    val descriptionContains: DescriptionFlags = checkDescriptionContent(description)
    val memoContains: MemoFlags = checkMemoContent(memo)

    val category: String =
      if (descriptionContains.DIVIDEND || descriptionContains.DEBIT_CARD_REVERSAL ||
        (descriptionContains.ELECTRONIC_DEPOSIT && (memoContains.US_FEDERAL_CU || memoContains.US_FCU || memoContains.CAPITAL_ONE_NA)))
        CATEGORY_DEPOSIT
      else if (descriptionContains.ELECTRONIC_WITHDRAWAL &&
        (memoContains.CAPITAL_ONE_CARD || memoContains.SAMS_CLUB_MC || memoContains.TARGET_CARD || memoContains.KOHLS_ONLINE))
        CATEGORY_CREDIT_CARD
      else if (descriptionContains.SHARE_DRAFT ||
        (descriptionContains.ELECTRONIC_WITHDRAWAL &&
          (memoContains.CTRPT_ENGY || memoContains.SHAKOPEE_PUBLIC_ONLINE || memoContains.COMCAST_ONLINE)))
        CATEGORY_UTILITY
      else if ((descriptionContains.ELECTRONIC_WITHDRAWAL &&
        (memoContains.AFFINITY_PLUS || memoContains.CORNERSTONE_MGMTONLINE || memoContains.SCOTT_COUNTY_ONLINE)) ||
        (descriptionContains.DEBIT_CARD_DEBIT && memoContains.THE_HOME_DEP))
        CATEGORY_HOME
      else CATEGORY_GENERAL_EXPENSE

    return category
  }

  private def checkDescriptionContent(description: String): DescriptionFlags = {
    val debitCardDebit = if (description.toUpperCase().contains(DEBIT_CARD_DEBIT)) true else false
    val dividend = if (description.toUpperCase().contains(DIVIDEND)) true else false
    val electronicDeposit = if (description.toUpperCase().contains(ELECTRONIC_DEPOSIT)) true else false
    val electronicWithdrawal = if (description.toUpperCase().contains(ELECTRONIC_WITHDRAWAL)) true else false
    val shareDraft = if (description.toUpperCase().contains(SHARE_DRAFT)) true else false
    val debitCardReversal = if (description.toUpperCase().contains(DEBIT_CARD_REVERSAL)) true else false
    val withdrawal = if (description.toUpperCase().contains(WITHDRAWAL)) true else false

    val descriptionFlags: DescriptionFlags = DescriptionFlags(debitCardDebit, dividend, electronicDeposit, electronicWithdrawal, shareDraft, debitCardReversal, withdrawal)
    descriptionFlags
  }

  private def checkMemoContent(memo: String): MemoFlags = {
    val usFederalCU = if (memo.toUpperCase().contains(US_FEDERAL_CU)) true else false
    val capitalOneNA = if (memo.toUpperCase().contains(CAPITAL_ONE_NA)) true else false
    val affinityPlus = if (memo.toUpperCase().contains(AFFINITY_PLUS)) true else false
    val comcastOnline = if (memo.toUpperCase().contains(COMCAST_ONLINE)) true else false
    val kohlsOnline = if (memo.toUpperCase().contains(KOHLS_ONLINE)) true else false
    val capitalOneCard = if (memo.toUpperCase().contains(CAPITAL_ONE_CARD)) true else false
    val samsClubMC = if (memo.toUpperCase().contains(SAMS_CLUB_MC)) true else false
    val targetCard = if (memo.toUpperCase().contains(TARGET_CARD)) true else false
    val ctrptEngy = if (memo.toUpperCase().contains(CTRPT_ENGY)) true else false
    val theHomeDep = if (memo.toUpperCase().contains(THE_HOME_DEP)) true else false
    val usFCU = if (memo.toUpperCase().contains(US_FCU)) true else false
    val cornerstoneMgmtOnline = if (memo.toUpperCase().contains(CORNERSTONE_MGMTONLINE)) true else false
    val shakopeePublicOnline = if (memo.toUpperCase().contains(SHAKOPEE_PUBLIC_ONLINE)) true else false
    val scottCountyOnline = if (memo.toUpperCase().contains(SCOTT_COUNTY_ONLINE)) true else false

    val memoFlags: MemoFlags = MemoFlags(usFederalCU, capitalOneNA, affinityPlus, comcastOnline, kohlsOnline, capitalOneCard, samsClubMC, targetCard,
      ctrptEngy, theHomeDep, usFCU, cornerstoneMgmtOnline, shakopeePublicOnline, scottCountyOnline)

    memoFlags
  }
}