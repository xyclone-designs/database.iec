using DataProcessor.Tables;

using SQLite;

using System.Linq.Expressions;

namespace DataProcessor
{
    public static class SQLiteConnectionExtensions
    {
        public static T CreateAndAdd<T>(this SQLiteConnection sqliteConnection, T t) where T : ElectionsItem, new()
        {
            sqliteConnection.Insert(t);

            return sqliteConnection.Table<T>().Last();
        }
        public static T FindOrCreateAndAdd<T>(this SQLiteConnection sqliteConnection, Expression<Func<T, bool>> predicate, Action<T> onCreate) where T : ElectionsItem, new()
        {
            T? t = null;
            try { t = sqliteConnection.Find<T>(predicate); } catch (Exception) { }
            t ??= ((IEnumerable<T>)sqliteConnection.Table<T>()).FirstOrDefault(predicate.Compile());
            if (t is not null) return t;

            onCreate.Invoke(t = new T());
            sqliteConnection.Insert(t);

            return sqliteConnection.Table<T>().Last();
        }
        public static T FindOrCreateAndAdd<T>(this SQLiteConnection sqliteConnection, int? pk, Action<T> onCreate) where T : ElectionsItem, new()
        {
            T? t;

            if (pk is null)
            {
                onCreate.Invoke(t = new T());
                sqliteConnection.Insert(t);

                return sqliteConnection.Table<T>().Last();
            }

            t = sqliteConnection.Find<T>(pk);

            if (t is null)
            {
                onCreate.Invoke(t = new T() { pk = pk.Value });
                sqliteConnection.Insert(t);
            }

            return t;
        }
    }
}
