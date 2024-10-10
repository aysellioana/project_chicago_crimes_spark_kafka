import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.SparkSession
import scalaj.http.{Http, HttpResponse}

class CrimeApiService(apiUrl: String, appToken: String, objectMapper: ObjectMapper)(implicit spark: SparkSession) {

  def fetchCrimeData(limit: Int, offset: Int): (Int, List[Crime]) = {
    try {
      val selectedColumns = "id,case_number,date,block,iucr,primary_type,description,location_description," +
        "arrest,domestic,beat,district,ward,community_area,fbi_code,year"

      // HTTP Request to API
      val response: HttpResponse[String] = Http(s"$apiUrl&$$limit=$limit&$$offset=$offset&$$select=$selectedColumns")
        .header("X-App-Token", appToken)
        .timeout(connTimeoutMs = 10000, readTimeoutMs = 20000)
        .asString

      if (response.code == 200 && response.body.nonEmpty) {
        // Deserialize JSON response to List[Crime] using ObjectMapper
        val crimeRecords: List[Crime] = objectMapper.readValue(response.body, new TypeReference[List[Crime]] {})
        (response.code, crimeRecords)
      } else {
        (response.code, List.empty[Crime])
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        (500, List.empty[Crime])
    }
  }
}
