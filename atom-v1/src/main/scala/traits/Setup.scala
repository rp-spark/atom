package traits

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

trait Setup {
  var sc: SparkContext = _

  def setup(outputPath: String): Unit = {
    val conf = new SparkConf()
    sc = new SparkContext(conf.setAppName("Atom"))

    val hadoopConf = sc.hadoopConfiguration
    val fs = FileSystem.get(hadoopConf)
    val path = new Path(outputPath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }

  }
}