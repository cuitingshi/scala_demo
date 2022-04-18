import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

//实验二 编写独立应用程序实现求二次排序问题

//将以下3个file中的成绩合并到同一个文件中，需要按照学生编号升序排序，若学生编号相同，则按照成绩降序排列，最后将结果输出到一个新文件中
//其中，每个输入文件表示班级学生某个学科的成绩，每行内容由4个字段组成，第一个是学生编号，第二个是学生名字，第三个是科目名称，第4个是学生成绩
object Task2 {
  class StudentScore(val studentNo: Int, val score: Int) extends Ordered[StudentScore] with Serializable {
    override def compare(that: StudentScore): Int = {
      if (this.studentNo != that.studentNo) {
        this.studentNo - that.studentNo
      }else {
        that.score - this.score
      }
    }
  }
  def main(args: Array[String]): Unit = {
    //1. 初始化spark context
    val sparkClusterAddr = "local" //TODO:换成真实的spark集群地址 spark://ting:7077
    val conf = new SparkConf().setAppName("TASK2").setMaster(sparkClusterAddr)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")


    //2. 读取文件，合并、二次排序
    val inputStudentScoreFileDir = "/tmp/student_score_files/*.txt" //TODO: 学生成绩的文件File1.txt、File2.txt、File3.txt都放在这个目录下
    val mergedStudentScoreFile = "/tmp/merged_student_score.txt"
    val inputRDD = sc.wholeTextFiles(inputStudentScoreFileDir, 3)
      .map(line => line._2)
      .coalesce(1)
      .map(content => {
        content.split("\n")
      }).flatMap( lines => lines)
      .map(line => {
        val arrs = line.split(" ")
        val studentNo = arrs(0).toInt
        val studentName = arrs(1)
        val course = arrs(2)
        val score = arrs(3).toInt
       // println("原始输入", line)
        (studentNo, studentName, course, score)
      }).sortBy(x => new StudentScore(x._1, x._4), true)
      .map(x => {
       // println("排序后的输入：", x)
        x._1 + ","+x._2 + "," + x._3 + ","+ x._4
      })
    //调试专用
    println("文件合并及二次排序后的结果存储在路径：",  mergedStudentScoreFile)
    println("进行二次排序后的结果为：")
    println(inputRDD.collect().mkString("\n"))

    //结果保存文件
    //inputRDD.saveAsTextFile(mergedStudentScoreFile)
    sc.stop()
  }
}

