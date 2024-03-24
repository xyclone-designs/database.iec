
namespace DataProcessor.Tables
{
    public class ElectionsItem
    {
        [SQLite.PrimaryKey, SQLite.NotNull, SQLite.AutoIncrement]
        public int pk { get; set; }
    }
}