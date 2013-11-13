using System;
using System.Collections.Generic;

namespace ServiceBatchTestsREST
{
  public class JSONRecordSet
  {
    public List<Attributes> records { get; set; }
  }

  public class Attributes
  {
    public Attributes(object attri)
    {
      attributes = attri;
    }
    public object attributes;
  }

  public class SingleLineAddress
  {
    public int ObjectID { get; set; }
    public String SingleLine { get; set; }
    public String CountryCode { get; set; }
  }

  public class MultiLineAddress
  {
    public int ObjectID { get; set; }
    public String Address { get; set; }
    public String Neighborhood { get; set; }
    public String City { get; set; }
    public String Subregion { get; set; }
    public String Region { get; set; }
    public String Postal { get; set; }
    public String PostalExt { get; set; }
    public String CountryCode { get; set; }
  }
}
