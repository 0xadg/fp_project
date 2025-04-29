package example
import java.io.PrintWriter
import com.github.nscala_time.time.Imports._
import sttp.client4.quick._
import sttp.client4.Response
import scala.collection.immutable.ListMap
import scala.io.Source


// simple data storage structure
// stores a day worth of data for specific generator type
// list map ensures the order is kept
class DataBatch(val genType: String,
                val values: ListMap[String, Double]) {
}

// handles data
object DataProvider {
    
    // writes all data from the batch to specified file
    def writeData(batch: DataBatch, path: String): Unit = {
      val writer = new PrintWriter(path)
      
      batch.values.map(x => {
        writer.println(x._1 + "," + x._2)
      })
      writer.close()
    }


    // opens a file and looks for a data on the provided date
    def readData(path: String, date: DateTime) : Unit = {
      // open file
      // get linecount
      // get 1st date
      // if 1st latter than the date - couldn't find data 
      // divide by 24
      // if non whole - error for data corruption?
      // get diff between 1st record date and provided date
      // if division res is less than diff - couldn't find the data
      val file = Source.fromFile(path)
      val lines = file.getLines().toArray
      if(lines.size % 24 != 0)
        println("Records miscount, data might be corrupted")
      
      val startDate = DateTime.parse(lines(0).split(",")(0))
      println("1st date in the file: " + startDate)

      val batch = new DataBatch(
        "wind",
        lines.map(x => {
          val items = x.split(",")
          (items(0), items(1).toDouble)
        }).to(ListMap)
      )
      
      batch.values.map(x => println(x))

    }
  // fetches a day worth of data (1-hour intervals)
    def requestData(date: DateTime, gtype: String): Option[DataBatch] = {
      // 75 - Wind power generation - 15 min data
      // 181 - Wind power production - real time data
      // 191 - Hydro power production - real time data
      // What we might use:
      // 245 - Wind power generation forecast - updated every 15 minutes
      // 248 - Solar power generation forecast - updated every 15 minutes
      // but no hydro prediction :(

        val genCode = gtype match {
          case "wind" => 245
          case "solar" => 248
        }
        
        val formatter = DateTimeFormat.forPattern("YYYY-MM-dd")
        val queryParams = Map(
          "startTime" -> (formatter.print(date) + "T00:00:00"),
          "format" -> "json",
          "pageSize" -> "96",
          "sortBy" -> "startTime",
          "sortOrder" -> "asc"
        )

        val response : Response[String] = quickRequest
        .get(uri"https://data.fingrid.fi/api/datasets/$genCode/data?$queryParams")
        .header("x-api-key", "<api key here>")
        .send()
        
        if(response.isSuccess)
        {
          val json = ujson.read(response.body)  
          val values = json("data").arr.map(x => x("value").num).toArray
          Some(new DataBatch(
              "wind",
              json("data").arr.zipWithIndex.collect {
                case (x, i) if i % 4 == 0 => (x("startTime").str, x("value").num)
              }.to(ListMap)
          ))
          
        }
        else
        {
          None
        }
    }
}

object REPS {
    def main(args: Array[String]): Unit = 
    {
        println("Text:")
        val text = scala.io.StdIn.readLine()
        menu(text)
    }

    def act1(): Unit = {
        
        DataProvider.requestData(DateTime.now, "wind") match
        {
          case Some(data) => data.values.map(x => println(x))
          case None => println("failed to fetch data.")
        }
        
    }
    def act2(): Unit = {
        DataProvider.readData("test.csv", DateTime.now)
    }

    // menu
    def menu(option: String): Unit = {
        option match {
            case "1" => act1()
            case "2" => act2()
            case "0" => println("Bye!")
            case _ => println("Command not recognized!")
        }
        if(option != "0") {
            menu(scala.io.StdIn.readLine())    
        }
    }
}