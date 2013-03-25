using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceBatchTestsREST
{
  class TestCase
  {
    /// <summary>
    /// Get or Set the ServerName
    /// </summary>
    public String ServerName { get; set; }

    /// <summary>
    /// Get or Set the batch App ID
    /// </summary>
    public String AppID { get; set; }

    /// <summary>
    /// Get or Set the batch App Secret
    /// </summary>
    public String AppSecret { get; set; }

    /// <summary>
    /// Get or Set the path to the Locator
    /// </summary>
    public String TableName { get; set; }

    /// <summary>
    /// Get or Set the path to the Address Table
    /// </summary>
    public String TablePath { get; set; }

    /// <summary>
    /// Get or Set the name of the output JSON file
    /// </summary>
    public String OutputPath { get; set; }

    /// <summary>
    /// Get or Set the batch size to send to the server
    /// </summary>
    public int BatchSizePerRequest { get; set; }

    /// <summary>
    /// Get or Set the UseMultiLine flag
    /// </summary>
    public bool UseMultiLine { get; set; }

    /// <summary>
    /// Get or Set the UseMultiLine flag
    /// </summary>
    public bool CreateSQLInsert { get; set; }

    /// <summary>
    /// Get the string of comma separated fields
    /// </summary>
    public String[] AddressFields { get; set; }

    /// <summary>
    /// Get the Single Line Field
    /// </summary>
    public String SingleLineField { get; set; }

    /// <summary>
    /// Get the Country Field
    /// </summary>
    public String Country { get; set; }

    /// <summary>
    /// Sets AddressFields from a List of address Fields
    /// </summary>
    /// <param name="addressFields"></param>
    public void setAddressFields(List<String> addressFields)
    {
      AddressFields = addressFields.ToArray<String>();
    }
  }
}
