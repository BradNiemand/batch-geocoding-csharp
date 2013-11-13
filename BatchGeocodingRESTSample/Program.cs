using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Web;
using System.Web.Script.Serialization;

namespace ServiceBatchTestsREST
{
  class BatchGeocoding
  {
    #region Program Member variables
    static String m_token = null;
    static String m_referer = System.Environment.MachineName;
    static String[] m_serviceFieldNames = { "SingleLine",
                                            "Address", 
                                            "Neighborhood",
                                            "City", 
                                            "Subregion", 
                                            "Region", 
                                            "Postal", 
                                            "PostalExt", 
                                            "Country" };
    #endregion
    #region User Member variables
    static String m_inputTableName = "addresses.csv";
    //static String m_inputTableName = "addressesUSFieldNames.csv";
    static String m_outputTableName = "Geocoding_result.csv";
    static String m_serverName = "geocode.arcgis.com";
    static String m_username = "";
    static String m_password = "";
    static int m_batchSize = 150;
    // Below there are two examples of how to specify address table inputs.  The values in the String
    // array are the column names in your table.  Array size is static for Single Field (2 values) and 
    // for Multiple Fields (8 values).  Not all values need to be populated, just the ones that correspond
    // to your input table.

    // <SINGLE FIELD EXAMPLE>  Address in Single field but with a separate field for Country
    // If no Country field, leave the value blank for the last field ("") instead of entering "Country"
    // Ex.  If you address table contains SingleLine, Country
    //static String[] m_addressFields = { "SingleLine",
    //                                    "", 
    //                                    "",
    //                                    "", 
    //                                    "", 
    //                                    "", 
    //                                    "", 
    //                                    "", 
    //                                    "Country" };
    // <MULTIPLE FIELDS EXAMPLE>  Address in multiple fields with an additional field for Country.
    // If no Country field, leave the value blank for the last field ("") instead of entering "Country"
    // Ex.  If you address table contains Address, Neighborhood, City, Subregion, Region, Postal, PostalExt, Country
    // This is what the included addresses.csv contains
    static String[] m_addressFields = { "",
                                        "Address", 
                                        "Neighborhood",
                                        "City", 
                                        "Subregion", 
                                        "Region", 
                                        "Postal", 
                                        "PostalExt", 
                                        "Country" };

    // Ex.  If you address table contains Address, City, State, Zip
    // This is what the addressesUSFieldNames.csv contains
    //static String[] m_addressFields = { "",
    //                                    "Address", 
    //                                    "",
    //                                    "City", 
    //                                    "", 
    //                                    "State", 
    //                                    "Zip", 
    //                                    "", 
    //                                    "" };
    #endregion

    static void Main(string[] args)
    {
      Console.WriteLine(m_inputTableName + " Executing...");
      String restURL = "https://" + m_serverName + "/arcgis/rest/services/World/GeocodeServer/geocodeAddresses";
      bool sslRequired = true;
      //m_token = GetTokenOAuth2(test.AppID, test.AppSecret, GetOAuth2TokenURL(test.ServerName), ref sslRequired);
      m_token = GetToken(m_username, m_password, GetTokenURL(m_serverName), ref sslRequired);
      List<GeocodedResult> geocodedResultList = null;

      // Populate the JSONRecordSet class to be used to Serialize into the JSON string
      JSONRecordSet recordSet = new JSONRecordSet();
      bool useSingleLine = false;
      if (m_addressFields[0] != "")
        useSingleLine = true;

      List<Attributes> addressTableList = readCSVRecordsAsList(m_inputTableName, m_addressFields, useSingleLine);

      // This while loop used to iterate through all of the batches of record sets
      int rowCount = addressTableList.Count;
      int batchSize = m_batchSize;
      int currentRow = 0;
      int startRow = 0;
      bool firstResult = true;
      while (currentRow < rowCount)
      {
        startRow = currentRow;
        currentRow += batchSize;
        if (currentRow >= rowCount)
        {
          batchSize = rowCount - startRow;
          currentRow = rowCount;
        }

        recordSet.records = addressTableList.GetRange(startRow, batchSize);

        // Create the POST string to pass to the server
        String postString = CreatePOSTString(recordSet);

        Stopwatch timer = new Stopwatch();
        timer.Start();
        // Make the request to the server
        String response = DoHttpRequest(restURL, "Addresses=" + HttpUtility.UrlEncode(postString), true, "POST", true);
        timer.Stop();
        Console.WriteLine("Elapsed time = " + timer.Elapsed);

        // Process the results
        if (response != null)
        {
          geocodedResultList = parseJsonResult(response);
          if (firstResult)
          {
            WriteResults(m_outputTableName, "ResultID,Loc_name,Status,Score,Match_addr,Addr_type,PlaceName,Rank,AddBldg,AddNum,AddNumFrom,AddNumTo,Side,StPreDir,StPreType,StName,StType,StDir,Nbrhd,City,Subregion,Region,Postal,PostalExt,Country,LangCode,Distance,X,Y,DisplayX,DisplayY,Xmin,Xmax,Ymin,Ymax", false);
            firstResult = false;
          }
          foreach (GeocodedResult geocodedResult in geocodedResultList)
          {
            WriteResults(m_outputTableName, GeocodedResultToString(geocodedResult), true);
          }
        }
        else
        {
          Console.WriteLine("An error occured processing the batch. See " + m_outputTableName + ".error for more information.");
          WriteResults(m_outputTableName + ".error", "Batch request with ID " + startRow + " through " + currentRow + " Failed.", true);
        }
        Console.WriteLine("Finished processing " + currentRow + " rows of " + rowCount);
      }
      Console.WriteLine("Batch geocoding completed.  Press Enter to continue...");
      Console.ReadLine();
    }

    /// <summary>
    /// Create the POST string from a batch of input addresses
    /// </summary>
    /// <param name="test"></param>
    /// <param name="currentRow"></param>
    /// <returns></returns>
    public static String CreatePOSTString(JSONRecordSet recordSet)
    {
      // Serialize into the JSON string
      JavaScriptSerializer jss = new JavaScriptSerializer();
      jss.MaxJsonLength = 2147483647; // 2147483647 is max int size
      String restRecordSetArray = jss.Serialize(recordSet);

      return restRecordSetArray;
    }

    /// <summary>
    /// Makes the Http request to the Service
    /// </summary>
    /// <param name="reqURL">The URL to the Service endpoint.  https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/geocodeAddresses.</param>
    /// <param name="reqString">The POST string in this example. Empty string for "GET" requests.</param>
    /// <param name="useGZip">Set to true if you would like the response to be zipped.</param>
    /// <param name="method">Either POST or GET</param>
    /// <returns>The HttpWebResponse String from the server.</returns>
    public static String DoHttpRequest(string reqURL, string reqString, bool useGZip, string method, bool useReferer = false)
    {
      HttpWebRequest request;
      if (method == "POST")
      {
        request = (HttpWebRequest)WebRequest.Create(reqURL);
        request.Credentials = CredentialCache.DefaultCredentials;
        request.Timeout = 1000 * 60 * 5;  // 5 minutes
        request.ReadWriteTimeout = 1000 * 60 * 5; // 5 minutes
        request.Method = method;
        if (useReferer)
          request.Referer = m_referer;

        byte[] byteArray = Encoding.UTF8.GetBytes(reqString + "&f=json&token=" + m_token);
        request.ContentType = "application/x-www-form-urlencoded";
        request.ContentLength = byteArray.Length;
        using (Stream dataStream = request.GetRequestStream())
        {
          dataStream.Write(byteArray, 0, byteArray.Length);
        }
      }
      else
      {
        request = (HttpWebRequest)WebRequest.Create(reqURL + "?f=json");
        request.Credentials = CredentialCache.DefaultCredentials;
        request.Timeout = 1000 * 60 * 5;  // 5 minutes
        request.ReadWriteTimeout = 1000 * 60 * 5; // 5 minutes
        request.Method = "GET";
      }

      if (useGZip)
        request.Headers.Add("Accept-Encoding: gzip");

      // Get the web response
      String result = null;
      try
      {
        using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
        {
          result = responseStreamToString(GetGzipStream(response));
        }
      }
      catch (WebException ex)
      {
        if (ex.Status == WebExceptionStatus.ProtocolError)
        {
          HttpWebResponse response2 = ex.Response as HttpWebResponse;
          if (response2 != null)
          {
            // Process response
          }
        }
      }
      return result;
    }

    /// <summary>
    /// Processes the JSON web response
    /// </summary>
    /// <param name="response">The HttpWebResponce from the request</param>
    /// <returns>A List of GeocodedResult objects is returned.</returns>
    private static List<GeocodedResult> parseJsonResult(String result)
    {
      // Deserialize the JSON result
      JavaScriptSerializer jss = new JavaScriptSerializer();
      jss.MaxJsonLength = 2147483647; // 2147483647 is max int size
      GeocodedFeatureSet gfs = jss.Deserialize<GeocodedFeatureSet>(result);

      return gfs.locations;
    }

    /// <summary>
    /// Turn the Stream into a String
    /// </summary>
    /// <param name="responseStream"></param>
    /// <returns>The String representation of the Stream</returns>
    private static String responseStreamToString(Stream responseStream)
    {
      String result = "";
      using (System.IO.StreamReader reader = new System.IO.StreamReader(responseStream))
      {
        result = reader.ReadToEnd();
      }

      return result;
    }

    /// <summary>
    /// Unzip the response if using GZip for the request.
    /// </summary>
    /// <param name="response"></param>
    /// <returns></returns>
    private static Stream GetGzipStream(HttpWebResponse response)
    {
      GZipStream zipStream = null;
      if (response.ContentEncoding == "gzip")
      {
        zipStream = new GZipStream(response.GetResponseStream(), CompressionMode.Decompress);
      }
      if (zipStream != null)
      {
        MemoryStream decompressedStream = new MemoryStream();
        int size = 2048;
        byte[] writeData = new byte[size];

        zipStream.Flush();

        while (true)
        {
          size = zipStream.Read(writeData, 0, size);
          if (size > 0)
          {
            decompressedStream.Write(writeData, 0, size);
          }
          else
          {
            break;
          }
        }
        decompressedStream.Flush();
        decompressedStream.Seek(0, SeekOrigin.Begin);
        return decompressedStream;
      }
      else
      {
        return response.GetResponseStream();
      }
    }

    /// <summary>
    /// Reads the records into the GeocodeAddressesRecordSet class.
    /// This class can then be passed to the JavaScriptSerializer to be serialized into a JSON string.
    /// </summary>
    public static List<Attributes> readCSVRecordsAsList(String table, String[] addressFields, bool useSingleLine = false)
    {
      List<Attributes> recordList = new List<Attributes>();

      // 32bit machine
      //String connectionString = @"Provider=Microsoft.Jet.OLEDB.4.0;" +
      //                          @"Data Source=" + System.IO.Directory.GetCurrentDirectory() + ";" +
      //                          @"Extended Properties=""text;HDR=YES;FMT=Delimited""";

      // 64bit machine
      String connectionString = @"Provider=Microsoft.ACE.OLEDB.12.0;" +
                          @"Data Source=" + System.IO.Directory.GetCurrentDirectory() + ";" +
                          @"Extended Properties=""text;HDR=YES;FMT=Delimited""";

      String multiLineInputString = "";
      List<String> tableFieldNames = new List<String>(addressFields);
      List<String> serviceFieldNames = new List<String>(m_serviceFieldNames);
      if (!useSingleLine)
      {
        for (int i = 0; i < tableFieldNames.Count; )
        {
          if (tableFieldNames[i] == "")
          {
            tableFieldNames.RemoveAt(i);
            serviceFieldNames.RemoveAt(i);
          }
          else
            i++;
        }
        multiLineInputString = String.Join(",", tableFieldNames.ToArray());
      }

      String queryString = "";
      if (useSingleLine)
      {
        queryString = "SELECT ObjectID," + addressFields[0];
        if (addressFields[1] != "")
          queryString += "," + addressFields[1];
        queryString += " FROM " + table;
      }
      else
        queryString = "SELECT ObjectID," + multiLineInputString +
                      " FROM " + table;

      using (OleDbConnection connection = new OleDbConnection())
      {
        connection.ConnectionString = connectionString;

        using (OleDbCommand command = connection.CreateCommand())
        {
          command.CommandText = queryString;
          connection.Open();

          using (OleDbDataReader dr = command.ExecuteReader())
          {
            try
            {
              while (dr.Read())
              {
                object address = null;
                if (useSingleLine)
                {
                  SingleLineAddress SLA = new SingleLineAddress();
                  SLA.ObjectID = dr.GetInt32(0);

                  // If null, insert empty string
                  int indexValue = serviceFieldNames.IndexOf("SingleLine");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    SLA.SingleLine = dr.GetString(indexValue + 1);
                  else
                    SLA.SingleLine = "";
                  indexValue = serviceFieldNames.IndexOf("CountryCode");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    SLA.CountryCode = dr.GetString(indexValue + 1);
                  else
                    SLA.CountryCode = "";

                  address = SLA;
                }
                else
                {
                  MultiLineAddress MLA = new MultiLineAddress();

                  // If null, insert empty string
                  MLA.ObjectID = dr.GetInt32(0);
                  int indexValue = serviceFieldNames.IndexOf("Address");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.Address = dr.GetString(indexValue + 1);
                  else
                    MLA.Address = "";
                  indexValue = serviceFieldNames.IndexOf("Neighborhood");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.Neighborhood = dr.GetString(indexValue + 1);
                  else
                    MLA.Neighborhood = "";
                  indexValue = serviceFieldNames.IndexOf("City");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.City = dr.GetString(indexValue + 1);
                  else
                    MLA.City = "";
                  indexValue = serviceFieldNames.IndexOf("Subregion");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.Subregion = dr.GetString(indexValue + 1);
                  else
                    MLA.Subregion = "";
                  indexValue = serviceFieldNames.IndexOf("Region");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.Region = dr.GetString(indexValue + 1);
                  else
                    MLA.Region = "";
                  indexValue = serviceFieldNames.IndexOf("Postal");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.Postal = dr.GetString(indexValue + 1);
                  else
                    MLA.Postal = "";
                  indexValue = serviceFieldNames.IndexOf("PostalExt");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.PostalExt = dr.GetString(indexValue + 1);
                  else
                    MLA.PostalExt = "";
                  indexValue = serviceFieldNames.IndexOf("CountryCode");
                  if (indexValue != -1 && !dr.IsDBNull(indexValue + 1))
                    MLA.CountryCode = dr.GetString(indexValue + 1);
                  else
                    MLA.CountryCode = "";

                  address = MLA;
                }

                recordList.Add(new Attributes(address));
              }
            }
            finally
            {
              dr.Close();
              connection.Close();
            }
          }
        }
      }

      return recordList;
    }

    /// <summary>
    /// Get a token that is required for batch geocoding.
    /// </summary>
    /// See http://developers.arcgis.com/en/authentication/app-logins.html  for information about App ID and App Secret
    /// <param name="appID">App ID</param>
    /// <param name="appSecret">App Secret</param>
    /// <param name="tokenURL">The token URL for the service</param>
    /// <param name="sslRequired">Determine if SSL is required</param>
    /// <returns>The token</returns>
    public static string GetTokenOAuth2(String appID, String appSecret, String tokenURL, ref bool sslRequired)
    {
      // get a token for the secure AGS services

      string newToken = "";
      string tokenReqTokenParams = "?client_id=" + appID + "&client_secret=" + appSecret + "&grant_type=client_credentials&f=json";

      HttpWebRequest request = (HttpWebRequest)WebRequest.Create(tokenURL + tokenReqTokenParams);
      ASCIIEncoding encoding = new ASCIIEncoding();

      request.Credentials = CredentialCache.DefaultCredentials;
      request.Timeout = 1000 * 60 * 5;  // 5 minutes
      request.Method = "POST";
      request.ContentType = "application/x-www-form-urlencoded";

      String tokenText = null;
      using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
      {
        tokenText = responseStreamToString(response.GetResponseStream());
      }
      //responseJSON
      JavaScriptSerializer jss = new JavaScriptSerializer();
      var responseJSON = jss.DeserializeObject(tokenText);
      object token;
      ((IDictionary<String, object>)responseJSON).TryGetValue("access_token", out token);

      newToken = (String)token;
      
      return newToken;
    }

    /// <summary>
    /// Get a token that is required for batch geocoding.
    /// </summary>
    /// See http://developers.arcgis.com/en/authentication/app-logins.html  for information about App ID and App Secret
    /// <param name="appID">App ID</param>
    /// <param name="appSecret">App Secret</param>
    /// <param name="tokenURL">The token URL for the service</param>
    /// <param name="sslRequired">Determine if SSL is required</param>
    /// <returns>The token</returns>
    public static string GetToken(String username, String password, String tokenURL, ref bool sslRequired)
    {
      // get a token for the secure AGS services

      string newToken = "";
      string tokenReqTokenParams = "?username=" + username + "&password=" + password + "&expiration=600&client=referer&referer="+ m_referer + "&f=json";

      HttpWebRequest request = (HttpWebRequest)WebRequest.Create(tokenURL + tokenReqTokenParams);
      ASCIIEncoding encoding = new ASCIIEncoding();

      request.Credentials = CredentialCache.DefaultCredentials;
      request.Timeout = 1000 * 60 * 5;  // 5 minutes
      request.Method = "POST";
      request.ContentType = "application/x-www-form-urlencoded";

      String tokenText = null;
      using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
      {
        tokenText = responseStreamToString(response.GetResponseStream());
      }

      //responseJSON
      JavaScriptSerializer jss = new JavaScriptSerializer();
      var responseJSON = jss.DeserializeObject(tokenText);
      object token;
      ((IDictionary<String, object>)responseJSON).TryGetValue("token", out token);

      newToken = (String)token;

      return newToken;
    }

    /// <summary>
    /// Get the token URL for a service.
    /// </summary>
    /// <param name="serverName">The service name</param>
    /// <returns>The URL to get the token for batch geocoding</returns>
    public static String GetOAuth2TokenURL(String serverName)
    {
      String newTokenURL = "";

      HttpWebRequest request = (HttpWebRequest)WebRequest.Create("http://" + serverName + "/arcgis/rest/info?f=json");
      ASCIIEncoding encoding = new ASCIIEncoding();

      request.Credentials = CredentialCache.DefaultCredentials;
      request.Timeout = 1000 * 60 * 5;  // 5 minutes
      request.Method = "POST";

      String responseText = null;
      using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
      {
        responseText = responseStreamToString(response.GetResponseStream());
      }

      //responseJSON
      JavaScriptSerializer jss = new JavaScriptSerializer();
      var responseJSON = jss.DeserializeObject(responseText);
      object owningSystemUrl;
      ((IDictionary<String, object>)responseJSON).TryGetValue("owningSystemUrl", out owningSystemUrl);
      newTokenURL = (String)owningSystemUrl + "/sharing/oauth2/token";

      Debug.WriteLine("New Token URL: " + newTokenURL);

      return newTokenURL;
    }

    /// <summary>
    /// Get the token URL for a service.
    /// </summary>
    /// <param name="serverName">The service name</param>
    /// <returns>The URL to get the token for batch geocoding</returns>
    public static String GetTokenURL(String serverName)
    {
      String newTokenURL = "";

      HttpWebRequest request = (HttpWebRequest)WebRequest.Create("http://" + serverName + "/arcgis/rest/info?f=json");
      ASCIIEncoding encoding = new ASCIIEncoding();

      request.Credentials = CredentialCache.DefaultCredentials;
      request.Timeout = 1000 * 60 * 5;  // 5 minutes
      request.Method = "POST";

      String responseText = null;
      using (HttpWebResponse response = (HttpWebResponse)request.GetResponse())
      {
        responseText = responseStreamToString(response.GetResponseStream());
      }

      //responseJSON
      JavaScriptSerializer jss = new JavaScriptSerializer();
      var responseJSON = jss.DeserializeObject(responseText);
      object authInfo, tokeServiceURL;
      ((IDictionary<String, object>)responseJSON).TryGetValue("authInfo", out authInfo);
      ((IDictionary<String, object>)authInfo).TryGetValue("tokenServicesUrl", out tokeServiceURL);

      newTokenURL = (String)tokeServiceURL;

      return newTokenURL;
    }

    /// <summary>
    /// Turns a geocoded result into a comma separated string to write to a CSV file.
    /// </summary>
    /// <param name="geocodedResult">Result class that should be converted into a String</param>
    /// <returns>A comma separated string of values from the geocoded result.</returns>
    public static String GeocodedResultToString(GeocodedResult geocodedResult)
    {
      StringBuilder sb = new StringBuilder();
      GeocodedFields results = geocodedResult.attributes;
      sb.Append(results.ResultID.ToString() + ",");
      sb.Append(results.Loc_name + ",");
      sb.Append(results.Status + ",");
      sb.Append(results.Score.ToString() + ",");
      sb.Append("\"" + results.Match_addr + "\",");
      sb.Append(results.Addr_type + ",");
      sb.Append(results.PlaceName + ",");
      sb.Append(results.Rank + ",");
      sb.Append(results.AddBldg + ",");
      sb.Append(results.AddNum + ",");
      sb.Append(results.AddNumFrom + ",");
      sb.Append(results.AddNumTo + ",");
      sb.Append(results.Side + ",");
      sb.Append(results.StPreDir + ",");
      sb.Append(results.StPreType + ",");
      sb.Append(results.StName + ",");
      sb.Append(results.StType + ",");
      sb.Append(results.StDir + ",");
      sb.Append(results.Nbrhd + ",");
      sb.Append(results.City + ",");
      sb.Append(results.Subregion + ",");
      sb.Append(results.Region + ",");
      sb.Append(results.Postal + ",");
      sb.Append(results.PostalExt + ",");
      sb.Append(results.Country + ",");
      sb.Append(results.LangCode + ",");
      sb.Append(results.Distance.ToString() + ",");
      sb.Append(results.X.ToString() + ",");
      sb.Append(results.Y.ToString() + ",");
      sb.Append(results.DisplayX.ToString() + ",");
      sb.Append(results.DisplayY.ToString() + ",");
      sb.Append(results.Xmin.ToString() + ",");
      sb.Append(results.Xmax.ToString() + ",");
      sb.Append(results.Ymin.ToString() + ",");
      sb.Append(results.Ymax.ToString());

      return sb.ToString();
    }

    /// <summary>
    /// Write the results to a file
    /// </summary>
    /// <param name="fileName">The full path with filename to write the results to.</param>
    /// <param name="result">The result to be written out to file.</param>
    /// <param name="append">Results will be appended if true. File will be recreated if false.</param>
    public static void WriteResults(String fileName, String result, bool append)
    {
      TextWriter log = new System.IO.StreamWriter(fileName, append, Encoding.UTF8);

      TextWriter threadSafeLog = TextWriter.Synchronized(log);
      threadSafeLog.WriteLine(result);
      threadSafeLog.Flush();
      threadSafeLog.Close();
    }
  }
}
