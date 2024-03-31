
namespace DataProcessor.Tables
{
    [SQLite.Table("votingDistricts")]
    public class VotingDistrict : ElectionsItem
    {
        public int? pkMunicipality { get; set; }
        public int? pkProvince { get; set; }
        public int? pkWard { get; set; }
        [SQLite.Unique]
        public string? id { get; set; }
    }
}