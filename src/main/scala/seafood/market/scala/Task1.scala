import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//实验一问题1： Spark-shell交互式编程+HDFS操作
object Task1 {
  def main(args: Array[String]): Unit = {
    val sparkClusterAddr = "local" //TODO:换成真实的spark集群地址
    val studentScoreFilePath = "/tmp/chapter5-data1.txt" //TODO: 这里
    val classFilePath = "/tmp/class.txt" //TODO: 换成真实的HDFS集群分班表的保存地址

    //实验1-问题1
    //1. 读取数据集合
    val conf = new SparkConf().setAppName("task1-1-1").setMaster(sparkClusterAddr)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //2. 解析每一行数据，转换成班级编号、姓名，然后去重、排序
    val inputRDD = sc.textFile(studentScoreFilePath)
    val classRDD = inputRDD.map(x => {val arrs = x.split(","); arrs(0)})
      .distinct().map(name => {
      if (name.toLowerCase.head.toInt <= 'g'.toInt) {
        (1, name)
      }else if (name.toLowerCase.head.toInt <= 'n'.toInt){
        (2, name)
      }else if (name.toLowerCase.head.toInt <= 't'.toInt) {
        (3, name)
      }else {
        (4, name)
      }
    }).sortBy(_._2, true)
      .map( x =>x._1 + "," + x._2)
    println("分班表为：")
    classRDD.foreach(println)
    //分班表保存文件
    classRDD.saveAsTextFile(classFilePath)


    //实验1-问题2
    val courseClassScoreFilePath = "/tmp/task1_2_course_class_score.txt" //这里换成你想要保存的HDFS路径
    val dataRDD = inputRDD.map(x => {
      val arrs = x.split(",")
      val name = arrs(0)
      val course = arrs(1)
      val score = arrs(2).toInt
      (name, (course, score))
    })
    println("姓名，（课程，分数）")
    dataRDD.foreach(println)
    val nameToClassRDD = classRDD.map(x => {
      val arrs = x.split(",")
      val classNo = arrs(0)
      val name = arrs(1)
      (name,classNo)
    }
    )
    val joinedRDD = dataRDD.leftOuterJoin(nameToClassRDD)
    val courseScoreRDD = joinedRDD.map(x => {
      val name = x._1
      val course = x._2._1._1
      val score = x._2._1._2
      val classNo = x._2._2.get
      val count = 1
      (course, classNo, name, score, count)
    }).keyBy(x => (x._1, x._2))
      .reduceByKey((x, y) => {
        (x._1, x._2, "", x._4 + y._4, x._5 + y._5)
      })
      .map(x => {
        (x._1._1, x._1._2, x._2._4.toDouble / x._2._5.toDouble)
      })
      .filter(x => {
        if (x._3 > 50) true else false
      }).sortBy(x => x._1, false)
      .map(x=> {
      x._1 + "," + x._2 + "," + x._3
    })

    println("课程-班级平均分大于50分的为：")
    courseScoreRDD.foreach(println)

    //courseScoreRDD.saveAsTextFile(courseClassScoreFilePath)

    sc.stop()
   }
}
