
namespace DataProcessor.Tables
{
    [SQLite.Table("wards")]
    public class Ward : ElectionsItem
    {
        public int? pkMunicipality { get; set; }
        public int? pkProvince { get; set; }
        [SQLite.Unique]
        public int? id { get; set; }
    }
}