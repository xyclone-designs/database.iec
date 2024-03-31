
namespace DataProcessor.Tables
{
    [SQLite.Table("wards")]
    public class Ward : ElectionsItem
    {
        public int? pkMunicipality { get; set; }
        public int? pkProvince { get; set; }
        public string? list_pkVotingDistrict { get; set; }
        [SQLite.Unique]
        public string? id { get; set; }
    }
}