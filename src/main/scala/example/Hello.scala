package example
import java.io.PrintWriter
import java.io.File
import com.github.nscala_time.time.Imports._
import sttp.client4.quick._
import sttp.client4.Response
import scala.collection.immutable.ListMap
import scala.io.Source
import scala.concurrent._
import ExecutionContext.Implicits.global

// simple data storage structure
// stores a day worth of data for specific generator type
// list map ensures the order is kept
class DataBatch(val genType: String,
                val values: ListMap[String, Double]) {
}

// handles data
object DataProvider {
    
  // writes data to file
  def writeData(data: Either[String, DataBatch], path: String): Unit = {
    val file = new File(path)
    val writer = if(file.exists()) new PrintWriter(file) else new PrintWriter(path)

    data match {
      case Left(x) => writer.println(x)
      case Right(x) => {
        x.values.map(y => {
          writer.println(y._1 + "," + y._2)
        })
      }
    }
    writer.close()
  }

  // reads data from file, returns a batch
  def readData(path: String, gtype: String): DataBatch = {
    val file = Source.fromFile(path)
    val lines = file.getLines().toArray
    if(lines.size % 24 != 0)
      println("Records miscount, data might be corrupted")
    
    val startDate = DateTime.parse(lines(0).split(",")(0))
    println("1st date in the file: " + startDate)

    new DataBatch(
      gtype,
      lines.map(x => {
        val items = x.split(",")
        (items(0), items(1).toDouble)
      }).to(ListMap)
    )
  }

  
  // requests data from API and returns a batch or nothing if retrieval failed
  def requestData(gtype: String, date: DateTime, size: Int, skip: Int, order: String = "desc", isForecast: Boolean = false): Option[DataBatch] = {
    
    val genCode = gtype match {
      case "wind_rtd" => 181
      case "wind_for" => 245
      case "solar_for" => 248
      case "hydro_rtd" => 191
    }
    
    // 2 different maps for a real-time-data calls
    // and forecast calls
    val formatter = DateTimeFormat.forPattern("YYYY-MM-dd'T'HH:mm:00")
    val queryParams = if(isForecast) Map(
        "format" -> "json",
        "pageSize" -> size.toString,
        "sortBy" -> "startTime",
        "sortOrder" -> order,
        "startTime" -> formatter.print(date)
      )
      else
        Map(
          "format" -> "json",
          "pageSize" -> size.toString,
          "sortBy" -> "startTime",
          "sortOrder" -> order
      )

    val response : Response[String] = quickRequest
      .get(uri"https://data.fingrid.fi/api/datasets/$genCode/data?$queryParams")
      .header("x-api-key", "<API KEY GOES HERE>")
      .send()
    
    if(response.isSuccess)
    {
      val json = ujson.read(response.body)  
      val values = json("data").arr.map(x => x("value").num).toArray
      Some(new DataBatch(
          gtype,
          json("data").arr.zipWithIndex.collect {
            case (x, i) if i % skip == 0 => (x("startTime").str, x("value").num)  // using skip to select only n-th records (useful for hourly data)
          }.to(ListMap)
      ))
    }
    else
    {
      None
    }
  } 

  // had to introduce this variable because couldn't
  // find any other way to stop the Future thing running
  // from main thread
  var run = true
  // and those 
  var windCurrent = ("NaN", 0.0)
  var solarCurrent = ("NaN", 0.0)
  var hydroCurrent = ("NaN", 0.0)

  // also all the data is 2-3hrs behind current time
  // and for some reason they can get stuck on some? whatever...
  def lookupActualData(): Unit = {
    //updating all values
    requestData("wind_rtd", DateTime.now.minusMinutes(3), 1, 1) match
    {
      case Some(data) => {
        windCurrent = data.values.head
      }
      case None => windCurrent = ("Failed to fetch data", 0)
    }

    requestData("hydro_rtd", DateTime.now.minusMinutes(3), 1, 1) match
    {
      case Some(data) => {
        hydroCurrent = data.values.head
      }
      case None => hydroCurrent = ("Failed to fetch data", 0)
    }

    requestData("solar_for", DateTime.now.minusMinutes(15), 1, 1, "asc", true) match
    {
      case Some(data) => {
        solarCurrent = data.values.head
      }
      case None => solarCurrent = ("Failed to fetch data", 0)
    }
    
    Thread.sleep(60000)
    if(run)
      lookupActualData()
  }

  // caches the forecasted data up to a month (28 days) back
  // if files exist - do nothing
  def cacheMonthlyData(): Unit = {
    // 
    val windfile = new File("wind.csv")
    if(!windfile.exists())
    {
      requestData("wind_for", DateTime.now.minusMonths(1).withTime(0,0,0,0), 2688, 4, "asc", true) match{
        case Some(data) => writeData(Right(data), "wind.csv")
        case None => print("ERROR: Failed to cache monthly data")
      }
    }
    else
    {
      println("Wind file found, skipping caching...")
    }

    val solarfile = new File("solar.csv")
    if(!solarfile.exists())
    {
      requestData("solar_for", DateTime.now.minusMonths(1).withTime(0,0,0,0), 2688, 4, "asc", true) match{
        case Some(data) => writeData(Right(data), "solar.csv")
        case None => print("ERROR: Failed to cache monthly data")
      }
    }
    else
    {
      println("Solar file found, skipping caching...")
    }

    // 

  }
}

object REPS {
    def main(args: Array[String]): Unit = 
    {
        println("Text:")
        val text = scala.io.StdIn.readLine()
        menu(text)
    }

  // sample method for starting data updater
    def act1(): Unit = {
        println("starting lookup function...")
        Future { DataProvider.lookupActualData() }
    }

    // sample method of displaying current data
    def act2(): Unit = {
      println("Current wind prod: " + DataProvider.windCurrent._2 + "MWh/h")
      println("Last update: " + DataProvider.windCurrent._1)
      println("Current hydro prod: " + DataProvider.hydroCurrent._2 + "MWh/h")
      println("Last update: " + DataProvider.hydroCurrent._1)
      println("Current solar prod: " + DataProvider.solarCurrent._2 + "Mw/h")
      println("Last update: " + DataProvider.solarCurrent._1)
    }

    // sample method for caching up data and reading it
    def act3(): Unit = {
      DataProvider.cacheMonthlyData()
      val batch = DataProvider.readData("wind.csv", "wind")
      println("cache size: " + batch.values.size)
    }

    // menu
    def menu(option: String): Unit = {
        option match {
            case "1" => act1()
            case "2" => act2()
            case "3" => act3()
            case "0" => 
            {
              println("Bye!")
              DataProvider.run = false
            }
            case _ => println("Command not recognized!")
        }
        if(option != "0") {
            menu(scala.io.StdIn.readLine())    
        }
    }
}