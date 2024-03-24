using Newtonsoft.Json.Linq;

namespace DataProcessor.Tables
{
    [SQLite.Table("municipalities")]
    public class Municipality : ElectionsItem
    {
        public int? pkProvince { get; set; }
        [SQLite.Unique]
        public string? name { get; set; }
        public string? nameLong { get; set; }
        public string? miifCategory { get; set; }
        public string? category { get; set; }
        public string? geoLevel { get; set; }
        [SQLite.Unique]
        public string? geoCode { get; set; }
        public bool? isDisestablished { get; set; }
        public string? addressEmail { get; set; }
        public string? addressPostal { get; set; }
        public string? addressStreet { get; set; }
        public string? numberPhone { get; set; }
        public string? numberFax { get; set; }
        public string? urlWebsite { get; set; }
        public string? urlLogo { get; set; }
        public int? population { get; set; }
        public decimal? squareKms { get; set; }
    }
}