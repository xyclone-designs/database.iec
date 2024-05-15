using System;

namespace DataProcessor.Tables
{
    [SQLite.Table("electoralEvents")]
    public class ElectoralEvent : ElectionsItem
    {
        public class Types
        {
            public const string Municipal = "municipal";
            public const string National = "national";
            public const string Provincial = "provincial";
        }

        public string? list_pkBallot { get; set; }
        public string? list_pkMunicipality_pkParty{ get; set; }
        public string? list_pkParty_designation_nationalAllocation { get; set; }
        public string? list_pkParty_idProvince_provincialAllocation { get; set; }
        public string? date { get; set; }
        public string? type { get; set; }

        public static string? Type(string? row)
        {
            return true switch
            {
                true when row is null => null,
                true when row.Contains(Types.Municipal, StringComparison.OrdinalIgnoreCase) => Types.Municipal,
                true when row.Contains(Types.National, StringComparison.OrdinalIgnoreCase) => Types.National,
                true when row.Contains(Types.Provincial, StringComparison.OrdinalIgnoreCase) => Types.Provincial,

                _ => null
            };
        }
        public static bool IsYear(int date, string? row)
        {
            return row?.Contains(date.ToString()) ?? false;
        }
        public static bool IsNational(string? row)
        {
            return row?.Contains(Types.National, StringComparison.OrdinalIgnoreCase) ?? false;
        }
        public static bool IsProvincial(string? row)
        {
            return row?.Contains(Types.Provincial, StringComparison.OrdinalIgnoreCase) ?? false;
        }
        public static bool IsMunicipal(string? row)
        {
            return row?.Contains(Types.Municipal, StringComparison.OrdinalIgnoreCase) ?? false;
        }
    }
}