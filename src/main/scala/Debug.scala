import com.google.gson.Gson
import domain.UserInput

/**
  * Created by sky on 2017/3/11.
  */
object Debug {
  def main(args: Array[String]): Unit = {
    val gson = new Gson()
    val userInput = new UserInput()
    userInput.setTaskID("1")
    userInput.setSex("female")
    userInput.setEndAge(40)
    userInput.setStartAge(20)
    userInput.setCities(Array("city6"))
    userInput.setSearchWords(Array("小米5"))
    println(gson.toJson(userInput))
    val data = gson.fromJson("{\"taskID\":\"123\",\"startAge\":12,\"sex\":\"female\",\"startDate\":\"2017-03-06\"}", classOf[UserInput])
    println(data.getStartDate)
  }
}
