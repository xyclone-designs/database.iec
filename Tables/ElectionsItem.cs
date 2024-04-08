
namespace DataProcessor.Tables
{
    public class ElectionsItem
    {
        [SQLite.PrimaryKey, SQLite.NotNull, SQLite.AutoIncrement, SQLite.Unique]
        public int pk { get; set; }
    }
}