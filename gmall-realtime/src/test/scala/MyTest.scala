import com.atguigu.gmall.realtime.util.MyESUtil
import io.searchbox.client.JestClient
import io.searchbox.core.{Get, Index, Search}

import scala.collection.mutable.ListBuffer

object MyTest {

    //测试
    def main(args: Array[String]): Unit = {

        val client: JestClient = MyESUtil.getClient
        //indexName    type    id    source
        val index: Index = new Index.Builder(Student("1001", "ZhangSan", 20))
            .index("student_test").`type`("_doc").id("2").build()

        val get: Get = new Get.Builder("student_test", "2").`type`("_doc").build()

        val search: Search = new Search.Builder("").addIndex("student_test").addType("_doc").build()

        val result = client.execute(search)
        val message: String = result.getErrorMessage

        println(result.getJsonString)
        println("message = " + message)
        MyESUtil.close(client)
    }

    case class Student(id: String, name: String, age: Int)

}

object MyTest1 {
    def main(args: Array[String]): Unit = {
        val strings = new ListBuffer[String]()
        "aaa"::strings.toList
        strings:+"bbb"
        strings+="ccc"
        strings.append("ddd")
        strings
        println(strings.mkString(" "))//ccc ddd
    }
}
