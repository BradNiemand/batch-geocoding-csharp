using System;
using System.Collections.Generic;
using System.Data.OleDb;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Web;
using System.Web.Script.Serialization;
using System.Xml;

namespace ServiceBatchTestsREST
{
  class BatchGeocoding
  {
    #region Member variables
    const String m_inputTestDataFileName = "TestData.xml";
    static String m_restURL = "";
    static String m_token = null;
    //static String m_referer = System.Environment.MachineName;
    #endregion

    static void Main(string[] args)
    {
      // Read the configuration from an XML file
      XmlReaderSettings settings = new XmlReaderSettings();
      settings.IgnoreWhitespace = true;
      settings.IgnoreComments = true;
      System.Xml.XmlReader xmlReader = System.Xml.XmlReader.Create(m_inputTestDataFileName, settings);
      TestCase[] testArray = readDataFromXML(xmlReader);

      bool firstResult = true;
      foreach (TestCase test in testArray)
      {
        Console.WriteLine(test.TableName + " Executing...");
        String restURL = "https://" + test.ServerName + "/arcgis/rest/services/World/GeocodeServer/geocodeAddresses";
        bool sslRequired = true;
        m_token = GetToken(test.AppID, test.AppSecret, GetTokenURL(test.ServerName), ref sslRequired);
        List<GeocodedResult> geocodedResultList = null;

        // Populate the JSONRecordSet class to be used to Serialize into the JSON string
        JSONRecordSet recordSet = new JSONRecordSet();
        List<Attributes> addressTableList = readCSVRecordsAsList(test);

        // This while loop used to iterate through all of the batches of record sets
        int rowCount = addressTableList.Count;
        int batchSize = test.BatchSizePerRequest;
        int currentRow = 0;
        int startRow = 0;
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

          // Make the request to the server
          HttpWebResponse response = DoHttpRequest(restURL, "Addresses=" + HttpUtility.UrlEncode(postString), true, "POST");

          // Process the results
          if (response != null)
          {
            geocodedResultList = parseJsonResult(response);
            if (firstResult)
            {
              WriteResults(test.OutputPath, "ResultID,Loc_name,Status,Score,Match_addr,Addr_type,PlaceName,Rank,AddBldg,AddNum,AddNumFrom,AddNumTo,Side,StPreDir,StPreType,StName,StType,StDir,Nbrhd,City,Subregion,Region,Postal,PostalExt,Country,LangCode,Distance,X,Y,DisplayX,DisplayY,Xmin,Xmax,Ymin,Ymax", false);
              firstResult = false;
            }
            foreach (GeocodedResult geocodedResult in geocodedResultList)
            {
              WriteResults(test.OutputPath, GeocodedResultToString(geocodedResult), true);
            }
          }
          else
          {
            Console.WriteLine("An error occured processing the batch. See " + test.OutputPath + ".error for more information.");
            WriteResults(test.OutputPath + ".error", "Batch request with ID " + startRow + " through " + currentRow + " Failed.", true);
          }

          Console.WriteLine("Finished processing " + currentRow + " rows of " + rowCount);
        }
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
    /// <returns>The HttpWebResponse from the server.</returns>
    public static HttpWebResponse DoHttpRequest(string reqURL, string reqString, bool useGZip, string method)
    {
      HttpWebRequest request;
      if (method == "POST")
      {
        request = (HttpWebRequest)WebRequest.Create(reqURL);
        request.Method = method;
        //request.Referer = m_referer;

        byte[] byteArray = Encoding.UTF8.GetBytes(reqString + "&f=json&token=" + m_token);
        request.ContentType = "application/x-www-form-urlencoded";
        request.ContentLength = byteArray.Length;
        Stream dataStream = request.GetRequestStream();
        dataStream.Write(byteArray, 0, byteArray.Length);
        dataStream.Close();
      }
      else
      {
        request = (HttpWebRequest)WebRequest.Create(reqURL + "?f=json");
        request.Method = "GET";
      }

      request.Credentials = CredentialCache.DefaultCredentials;
      request.Timeout = 1000 * 60 * 60 * 2;

      if (useGZip)
        request.Headers.Add("Accept-Encoding: gzip");

      // Get the web response
      HttpWebResponse response = null;
      try
      {
        response = (HttpWebResponse)request.GetResponse();
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

      return response;
    }

    /// <summary>
    /// Processes the JSON web response
    /// </summary>
    /// <param name="response">The HttpWebResponce from the request</param>
    /// <returns>A List of GeocodedResult objects is returned.</returns>
    private static List<GeocodedResult> parseJsonResult(HttpWebResponse response)
    {
      string result = responseStreamToString(GetGzipStream(response));

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
        reader.Close();
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
    /// Read Test data from an XML file
    /// </summary>
    /// <param name="xmlReader"></param>
    /// <returns>An array of the TestCase Object</returns>
    private static TestCase[] readDataFromXML(XmlReader xmlReader)
    {
      TestCase testCase = null;
      List<TestCase> testElements = new List<TestCase>();
      List<String> addressFields = new List<String>();

      while (xmlReader.Read())
      {
        if (xmlReader.IsStartElement())
        {
          if (xmlReader.IsStartElement("Test"))
          {
            if (testCase == null)
              testCase = new TestCase();
            // There is more than one test so push it onto the List
            else
            {
              testCase.setAddressFields(addressFields);
              testElements.Add(testCase);
              testCase = null;
              addressFields.Clear();
              testCase = new TestCase();
            }
          }
          else if (xmlReader.IsStartElement("ServerName"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.ServerName = xmlReader.ReadString();
          }
          else if (xmlReader.IsStartElement("AppID"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.AppID = xmlReader.ReadString();
          }
          else if (xmlReader.IsStartElement("AppSecret"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.AppSecret = xmlReader.ReadString();
          }
          else if (xmlReader.IsStartElement("AddressTableWorkspace"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.TablePath = xmlReader.ReadString();
          }
          else if (xmlReader.IsStartElement("AddressTableName"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.TableName = xmlReader.ReadString();
          }
          else if (xmlReader.IsStartElement("OutputPath"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.OutputPath = xmlReader.ReadString();
          }
          else if (xmlReader.IsStartElement("BatchSizePerRequest"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.BatchSizePerRequest = xmlReader.ReadContentAsInt();
          }
          else if (xmlReader.IsStartElement("UseMultiLine"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.UseMultiLine = xmlReader.ReadContentAsBoolean();
          }
          else if (xmlReader.IsStartElement("MultiLineInputs"))
          {
            xmlReader.Read(); // Read the Start Element
            addressFields.Add(xmlReader.ReadString());
          }
          else if (xmlReader.IsStartElement("SingleLineField"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.SingleLineField = xmlReader.ReadString();
          }
          else if (xmlReader.IsStartElement("Country"))
          {
            xmlReader.Read(); // Read the Start Element
            testCase.Country = xmlReader.ReadString();
          }
        }
      }
      testCase.setAddressFields(addressFields);
      testElements.Add(testCase);

      return testElements.ToArray<TestCase>();
    }

    /// <summary>
    /// Reads the records into the GeocodeAddressesRecordSet class.
    /// This class can then be passed to the JavaScriptSerializer to be serialized into a JSON string.
    /// </summary>
    public static List<Attributes> readCSVRecordsAsList(TestCase test)
    {
      List<Attributes> recordList = new List<Attributes>();

      // 32bit machine
      //String connectionString = @"Provider=Microsoft.Jet.OLEDB.4.0;" +
      //                          @"Data Source=" + test.TablePath + ";" +
      //                          @"Extended Properties=""text;HDR=YES;FMT=Delimited""";

      // 64bit machine
      String connectionString = @"Provider=Microsoft.ACE.OLEDB.12.0;" +
                          @"Data Source=" + test.TablePath + ";" +
                          @"Extended Properties=""text;HDR=YES;FMT=Delimited""";

      bool useSingleLine = !test.UseMultiLine;
      bool useCountryCode = test.Country == "" ? false : true;
      String multiLineInputString = "";
      foreach (String addressField in test.AddressFields)
      {
        if (multiLineInputString != "")
          multiLineInputString += ",";

        multiLineInputString += addressField;
      }
      int countryCodeIndex = test.AddressFields.Length + 1;

      String queryString = "";

      if (useSingleLine && !useCountryCode)
        queryString = "SELECT ObjectID," + test.SingleLineField +
                      " FROM " + test.TableName;
      else if (useSingleLine && useCountryCode)
        queryString = "SELECT ObjectID," + test.SingleLineField + "," + test.Country +
                      " FROM " + test.TableName;
      else if (!useSingleLine && !useCountryCode)
        queryString = "SELECT ObjectID," + multiLineInputString +
                      " FROM " + test.TableName;
      else if (!useSingleLine && useCountryCode)
        queryString = "SELECT ObjectID," + multiLineInputString + "," + test.Country +
                      " FROM " + test.TableName;

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
                  if (!dr.IsDBNull(1))
                    SLA.SingleLine = dr.GetString(1);
                  else
                    SLA.SingleLine = "";
                  if (useCountryCode && !dr.IsDBNull(2))
                    SLA.CountryCode = dr.GetString(2);
                  else
                    SLA.CountryCode = "";

                  address = SLA;
                }
                else
                {
                  MultiLineAddress MLA = new MultiLineAddress();

                  // If null, insert empty string
                  MLA.ObjectID = dr.GetInt32(0);
                  if (!dr.IsDBNull(1))
                    MLA.Address = dr.GetString(1);
                  else
                    MLA.Address = "";
                  if (!dr.IsDBNull(2))
                    MLA.Neighborhood = dr.GetString(2);
                  else
                    MLA.Neighborhood = "";
                  if (!dr.IsDBNull(3))
                    MLA.City = dr.GetString(3);
                  else
                    MLA.City = "";
                  if (!dr.IsDBNull(4))
                    MLA.Subregion = dr.GetString(4);
                  else
                    MLA.Subregion = "";
                  if (!dr.IsDBNull(5))
                    MLA.Region = dr.GetString(5);
                  else
                    MLA.Region = "";
                  if (!dr.IsDBNull(6))
                    MLA.Postal = dr.GetString(6);
                  else
                    MLA.Postal = "";
                  if (!dr.IsDBNull(7))
                    MLA.PostalExt = dr.GetString(7);
                  else
                    MLA.PostalExt = "";
                  if (useCountryCode && !dr.IsDBNull(countryCodeIndex))
                    MLA.CountryCode = dr.GetString(countryCodeIndex);
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
    public static string GetToken(String appID, String appSecret, String tokenURL, ref bool sslRequired)
    {
      // get a token for the secure AGS services

      string newToken = "";
      string tokenReqTokenParams = "?client_id=" + appID + "&client_secret=" + appSecret + "&grant_type=client_credentials&f=json";

      HttpWebRequest request = (HttpWebRequest)WebRequest.Create(tokenURL + tokenReqTokenParams);
      ASCIIEncoding encoding = new ASCIIEncoding();

      request.Credentials = CredentialCache.DefaultCredentials;
      request.Timeout = 1000 * 60 * 2;  // 2 minutes
      request.Method = "POST";
      request.ContentType = "application/x-www-form-urlencoded";

      HttpWebResponse response = (HttpWebResponse)request.GetResponse();

      String tokenText = responseStreamToString(response.GetResponseStream());
      response.Close();

      //responseJSON
      JavaScriptSerializer jss = new JavaScriptSerializer();
      var responseJSON = jss.DeserializeObject(tokenText);
      object token;
      ((IDictionary<String, object>)responseJSON).TryGetValue("access_token", out token);

      newToken = (String)token;

      return newToken;
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
      request.Timeout = 1000 * 60 * 2;  // 2 minutes
      request.Method = "POST";

      HttpWebResponse response = (HttpWebResponse)request.GetResponse();

      String responseText = responseStreamToString(response.GetResponseStream());
      response.Close();

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
