
namespace DataProcessor.Tables
{
    [SQLite.Table("provinces")]
    public class Province : ElectionsItem
    {
        public string? id { get; set; }
        public string? capital { get; set; }
        public string? name { get; set; }
        public int? population { get; set; }
        public int? squareKms { get; set; }
        public string? urlCoatOfArms { get; set; }
        public string? urlWebsite { get; set; }
    }
}