package example
import java.io.PrintWriter
import java.io.File
import com.github.nscala_time.time.Imports._
import sttp.client4.quick._
import sttp.client4.Response
import scala.util.Using
import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.Random
import scala.concurrent._
import ExecutionContext.Implicits.global

// done by Jere Seilo and Andrey Klenov

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
      .header("x-api-key", "536b9a78e52c47b2874fe816858e0adc")
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
  var windCurrent = ("Not updated", 0.0)
  var solarCurrent = ("Not updated", 0.0)
  var hydroCurrent = ("Not updated", 0.0)

  // data can lag behind current time by 2-3 hrs sometimes
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
    
    // power prod warning
    if(windCurrent._2 < 500)
    {
      println(Console.YELLOW + "\n\nWARNING:\n\tLow wind turbines output (<500 MWh/h)\n\tPlease check turbines status" + Console.RESET)
    }

    // random warnings
    if(Random.between(10, 30) > 27)
    {
      println(Console.YELLOW + "\n\nWARNING:\n\tHydro turbines synchronization fault\n\tPlease contact the maintenance team" + Console.RESET)
    }
    val roll2 = Random.between(15, 100)
    if(roll2 < 73 && roll2 > 65)
    {
      println(Console.RED + "\n\nERROR:\n\tEmergency shutdown was triggered in the turbine hall\n\tContact the maintenance team immediately" + Console.RESET)
    }

    Thread.sleep(60000)
    if(run)
      lookupActualData()
  }

  // caches the forecasted data up to a month (28 days) back
  // if files exist - do nothing
  def cacheMonthlyData(): Unit = {
    //
    println("\nStoring the data in a file, please wait...")

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

    Thread.sleep(5000)  //Had to add this 5sec sleep in between to avoid API rate limit

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

    Thread.sleep(5000) 

    val hydroFile = new File("hydro.csv")
    if(!hydroFile.exists())
    {
      requestData("wind_for", DateTime.now.minusMonths(1).withTime(0,0,0,0), 2688, 4, "asc", true) match{
        case Some(data) => writeData(
          Right(new DataBatch(
            data.genType,
            data.values.map(x => (x._1, x._2 * Random.between(0.44, 0.84)))
          )), 
          "hydro.csv")
        case None => print("ERROR: Failed to cache monthly data")
      }
    }
    else
    {
      println("Hydro file found, skipping caching...")
    }

    // 

  }
}

object REPS {
    def main(args: Array[String]): Unit = {
      println("\n--- Renewable Energy Plant System (REPS) ---")
      act1()
      menu()
    }

  // sample method for starting data updater
    def act1(): Unit = {
        println("Initializing monitoring function...")
        Future { DataProvider.lookupActualData() }
    }

    // sample method of displaying current data
    def act2(): Unit = {
      println("\nCurrent wind production: " + DataProvider.windCurrent._2 + " MWh/h")
      println("Last update timestamp: " + DataProvider.windCurrent._1 + "\n")
      println("Current hydro production: " + DataProvider.hydroCurrent._2 + " MWh/h")
      println("Last update timestamp: " + DataProvider.hydroCurrent._1 + "\n")
      println("Current solar production: " + DataProvider.solarCurrent._2 + " MWh/h")
      println("Last update timestamp: " + DataProvider.solarCurrent._1 + "\n")
    }

    // sample method for caching up data and reading it
    def act3(): Unit = {
      println("Caching data...")
      DataProvider.cacheMonthlyData()
      println("Data caching complete.")
    }

    // The behaviour to analyze data
    def act4(): Unit = {
      val fileNameOpt = selectGenType().flatMap(Utils.getFileName)

      fileNameOpt match {
        case Some(fp) =>
          Utils.extractValues(fp) match {
            case Right(values) =>
              val filter = askFilterType() // New: Ask before analysis
              Utils.analyzeData(values, filter)
            case Left(errorMsg) =>
              println(errorMsg)
              menu()
          }
        case None =>
          println("Invalid generator selection.")
          menu()
      }
    }

  // = = = = = = = = = =
  // Control functions for energy methods (These are just mock functions to imitate hardware control.)
  object EnergyControl {

    def adjustWindTurbine(): Unit =
      println("\nAdjusting wind turbine blades, based on wind direction... \nDone.\n")

    def troubleshootWindTurbine(): Unit = {
      println("\nTroubleshooting wind turbines...")
      Thread.sleep(1000)
      println("No errors found.\n")
    }

    def adjustSolarPanels(): Unit =
      println("\nTurning solar panels towards the sun... \nDone.\n")

    def troubleshootSolarPanels(): Unit = {
      println("\nTroubleshooting solar panels...")
      Thread.sleep(1000)
      println("No errors found.\n")
    }

    def adjustHydroPlant(): Unit =
      println("\nRegulating the flow rate...\nCompleted.\n")

    def troubleshootHydroPlant(): Unit = {
      println("\nTroubleshooting hydro turbines...")
      Thread.sleep(1000)
      println("No errors found.\n")
    }
  }




  // = = = = = = = = = =
  // MENUS (All the switch based menu selections)


  // Menu to select the Gen type
  def selectGenType(): Option[String] = {
    println("\nSelect energy source type:")
    println("1. Wind")
    println("2. Solar")
    println("3. Hydro")
    println("4. Back to Main Menu")
    println("Enter your choice: ")
    scala.io.StdIn.readLine match {
      case "1" => Some("wind")
      case "2" => Some("solar")
      case "3" => Some("hydro")
      case "4" => menu(); None
      case _ =>
        println("\nInvalid selection. Returning to main menu.\n")
        None
    }
  }


  // Menu for controlling energy methods
  def controlMenu(energyType: String)(adjustAction: => Unit, troubleshootAction: => Unit): Unit = {
    println(s"\n$energyType Control Menu:")
    println("1. Adjust Settings")
    println("2. Run automated troubleshooting")
    println("3. Back to Main Menu")
    print("Enter your choice: ")


    scala.io.StdIn.readLine() match {
      case "1" =>
        adjustAction
        controlMenu(energyType)(adjustAction, troubleshootAction)
      case "2" =>
        troubleshootAction
        controlMenu(energyType)(adjustAction, troubleshootAction)
      case "3" =>
        menu()
      case _ =>
        println("\nInvalid choice.\n")
        controlMenu(energyType)(adjustAction, troubleshootAction)
    }
  }

  // Selection menu for the filter type
  def askFilterType(): String = {
    println("\nChoose time filter:")
    println("1. Monthly")
    println("2. Weekly")
    println("3. Daily")
    println("4. Hourly")
    print("Enter your choice: ")

    scala.io.StdIn.readLine match {
      case "1" => "month"
      case "2" => "week"
      case "3" => "day"
      case "4" => "hour"
      case _ =>
        println("Invalid choice, defaulting to hourly.")
        "hour"
    }
  }


  // Main menu of REPS
  def menu(): Unit = {
    println("\n1. Control Energy Sources")
    println("2. Monitor Current Production")
    println("3. Store Historical Data in a File")
    println("4. Display and Analyze Data")
    println("5. Exit")
    print("Enter your choice: ")


    scala.io.StdIn.readLine() match {
      case "1" =>
        selectGenType() match {
          case Some("wind") =>
            controlMenu("Wind Turbine")(EnergyControl.adjustWindTurbine(), EnergyControl.troubleshootWindTurbine())
          case Some("solar") =>
            controlMenu("Solar")(EnergyControl.adjustSolarPanels(), EnergyControl.troubleshootSolarPanels())
          case Some("hydro") =>
            controlMenu("Hydro Plant")(EnergyControl.adjustHydroPlant(), EnergyControl.troubleshootHydroPlant())
          case None =>
            println("\nInvalid generator selection.\n")
        }
        menu()
      case "2" =>
        act2()
        menu()
      case "3" =>
        act3()
        menu()
      case "4" =>
        act4()
        menu()
      case "5" =>
        println("Shutting Down REPS!")
        System.exit(0)
      case _ =>
        println("\nCommand not recognized!\n")
        menu()
    }
  }
}

// Utility functions for handling data etc.
object Utils {
  //function to link energy method to the respective file
  def getFileName(genType: String): Option[String] = genType match {
    case "wind" => Some("wind.csv")
    case "solar" => Some("solar.csv")
    case "hydro" => Some("hydro.csv")
    case _ => None
  }

  // utility function to extract the values from the file and returning them as a listmap
  def extractValues(filePath: String): Either[String, ListMap[String, Double]] = {
    Using(Source.fromFile(filePath)) { source =>
      val data = source.getLines().map { line =>
        val columns = line.split(",")
        val timestamp = columns(0)
        val value = columns(1).toDouble
        (timestamp, value)
      }.toList

      // Sort the data by timestamp
      val sortedData = data.sortBy(_._1)
      // Convert sorted data to ListMap
      ListMap(sortedData: _*)
    }.toEither.left.map {
      case e: Exception => s"Failed to read file: ${e.getMessage}"
    }
  }

  // utility function the groups and sorts the data based on user given filter and calculating values for mean, mode etc.
  def analyzeData(data: ListMap[String, Double], resolution: String): Unit = {
    val grouped = data.groupBy { case (timestamp, _) =>
      val dt = DateTime.parse(timestamp)
      resolution match {
        case "month" => dt.toString("YYYY-MM")
        case "week" => dt.getWeekOfWeekyear.toString + "-" + dt.getYear.toString
        case "day" => dt.toString("YYYY-MM-dd")
        case "hour" => dt.toString("YYYY-MM-dd HH")
      }
    }

    println(s"\nAggregated data by $resolution:\n")

    grouped.toSeq.sortBy(_._1).foreach { case (time, entries) =>
      val values = entries.map(_._2).toList
      val total = values.sum

      if (values.size > 1) {
        println(f"\nTotal energy generated during $time = $total%.2f MWh")

        val mean     = Utils.mean(values)
        val median   = Utils.median(values)
        val mode     = Utils.mode(values)
        val range    = Utils.range(values)
        val midrange = Utils.midrange(values)

        def format(label: String, result: Either[String, Double]): String =
          result.fold(
            err => s"  $label: Error - $err",
            v   => f"  $label: $v%.2f MWh"
          )

        println(s"$time:")
        println(format("Mean", mean))
        println(format("Median", median))
        println(format("Mode", mode))
        println(format("Range", range))
        println(format("Midrange", midrange))

      } else {
        println(s"$time: = ${values.head} MWh")
      }
    }
  }

  // Functional error handling type alias (allows calculcation results to show the number format OR return error message as a string)
  type Result[A] = Either[String, A]

  // Mean
  def mean(values: List[Double]): Result[Double] = {
    @annotation.tailrec //to ensure tail recursion
    def recursiveFunc(lst: List[Double], accumulator: Double): Double = lst match { //sub function to use in recursion. Loop recursively until we have the sum of list contents in accumulator
      case Nil => accumulator
      case head :: tail => recursiveFunc(tail, accumulator + head)
    }
    values match {
      case Nil => Left("Cannot compute mean")
      case _ => Right(recursiveFunc(values, 0.0) / values.length) // after recursion is done divide accumulator by list length
    }
  }

  // Median
  def median(values: List[Double]): Result[Double] = {
    def sorted = values.sorted
    sorted match {
      case Nil => Left("Cannot compute median")
      case list =>
        val num = list.length
        val mid = num / 2
        if (num % 2 == 0)
          Right((list(mid - 1) + list(mid)) / 2.0)
        else
          Right(list(mid))
    }
  }

  // Mode
  def mode(values: List[Double]): Result[Double] = {
    @annotation.tailrec  //to ensure tail recursion
    def recursiveFunc(lst: List[Double], accumulator: Map[Double, Int]): Map[Double, Int] = lst match {
      case Nil => accumulator
      case head :: tail =>
        val count = accumulator.getOrElse(head, 0) + 1
        recursiveFunc(tail, accumulator + (head -> count))
    }

    values match {
      case Nil => Left("Cannot compute mode")
      case _ =>
        val freqMap = recursiveFunc(values, Map.empty)
        val maxFreq = freqMap.values.max
        val modes = freqMap.filter(_._2 == maxFreq).keys.toList
        Right(modes.head)
    }
  }

  // Range
  def range(values: List[Double]): Result[Double] = {
    @annotation.tailrec //to ensure tailrecursion
    def findMinMax(lst: List[Double], currMin: Double, currMax: Double): (Double, Double) = lst match { //recursive function that goes through the list recursively and updates currMin and currMax if ncessary
      case Nil => (currMin, currMax)
      case head :: tail =>
        val newMin = if (head < currMin) head else currMin
        val newMax = if (head > currMax) head else currMax
        findMinMax(tail, newMin, newMax)
    }

    values match {
      case Nil => Left("Cannot compute range")
      case head :: tail =>
        val (min, max) = findMinMax(tail, head, head)
        Right(max - min)
    }
  }

  // Midrange
  def midrange(values: List[Double]): Result[Double] = {
    range(values).map(r => r / 2.0 + values.min)
  }
}
