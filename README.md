batch-geocoding-csharp
======================
This sample shows how to use batch geocoding with geocode.arcigs.com.
  - Uses ArcGIS.com username and password to obtain a token
    *NOTE*  This app will use your ArcGIS.com credits.
  - Takes a CSV file of addresses and outputs a CSV file of geocoded locations.

1. Open the C# solution with VS2010
2. Open the Program.cs file
3. Near the top of the class there is a section with named User Member variables
4. Edit the m_username and m_password variables with your ArcGIS.com account
5. You can now run the program using the default csv file that is included (26 addresses)

After you have it running, you can now include your csv file
6. Choose/Edit one of the m_addressFields arrays that fits your input address table.