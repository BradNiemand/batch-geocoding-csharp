using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ServiceBatchTestsREST
{
  public class GeocodedFeatureSet
  {
    public List<GeocodedResult> locations { get; set; }
    public SpatialReference spatialReference { get; set; }
  }

  public class SpatialReference
  {
    public int wkid { get; set; }
    public int latestWkid { get; set; }
  }

  public class GeocodedResult
  {
    public GeocodedFields attributes;
    public String address { get; set;}
    public Location location;
    public double score { get; set; }
  }

  public class GeocodedFields
  {
    public int ResultID { get; set; }
    public String Loc_name { get; set; }
    public String Status { get; set; }
    public double Score { get; set; }
    public String Match_addr { get; set; }
    public String Addr_type { get; set; }
    public String PlaceName { get; set; }
    public String Rank { get; set; }
    public String AddBldg { get; set; }
    public String AddNum { get; set; }
    public String AddNumFrom { get; set; }
    public String AddNumTo { get; set; }
    public String Side { get; set; }
    public String StPreDir { get; set; }
    public String StPreType { get; set; }
    public String StName { get; set; }
    public String StType { get; set; }
    public String StDir { get; set; }
    public String Nbrhd { get; set; }
    public String City { get; set; }
    public String Subregion { get; set; }
    public String Region { get; set; }
    public String Postal { get; set; }
    public String PostalExt { get; set; }
    public String Country { get; set; }
    public String LangCode { get; set; }
    public double Distance { get; set; }
    public double X { get; set; }
    public double Y { get; set; }
    public double DisplayX { get; set; }
    public double DisplayY { get; set; }
    public double Xmin { get; set; }
    public double Xmax { get; set; }
    public double Ymin { get; set; }
    public double Ymax { get; set; }
  }

  public class Location
  {
    public double x { get; set; }
    public double y { get; set; }
  }
}
