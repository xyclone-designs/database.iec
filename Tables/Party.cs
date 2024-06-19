
namespace DataProcessor.Tables
{
    [SQLite.Table("parties")]
    public class Party : ElectionsItem
    {
        public string? list_pkElectoralEvent { get; set; }
        public string? name { get; set; }
        public string? abbr { get; set; }
        public string? dateEstablished { get; set; }
        public string? dateDisestablished { get; set; }
        public string? headquarters { get; set; }
        public string? urlWebsite { get; set; }
        public string? urlLogo { get; set; }
        public string? color { get; set; }
    }
}