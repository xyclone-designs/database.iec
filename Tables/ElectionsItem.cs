﻿using System.Linq;

namespace DataProcessor.Tables
{
    public class ElectionsItem
    {
        [SQLite.PrimaryKey, SQLite.NotNull, SQLite.AutoIncrement, SQLite.Unique]
        public int pk { get; set; }

		public static string AddPKIfUnique(string? pks, int? pk)
		{
			if (pk is null)
				return string.Empty;
			if (pks is null)
				pks = pk.Value.ToString();
			else if (pks.Split(",").Contains(pk.ToString()) is false)
				pks += string.Format(",{0}", pk);

			return pks;
		}
		public static string AddPKPairIfUnique(string? pkPairs, int pk, int value, bool addtoifpresent = false)
		{
			if (pkPairs is null)
				pkPairs = string.Format("{0}:{1}", pk, value);
			else if (pkPairs.Split(",").FirstOrDefault(_pkPair => long.Parse(_pkPair.Split(":")[0]) == pk) is not string pkPair)
				pkPairs += string.Format(",{0}:{1}", pk, value);
			else if (addtoifpresent)
				pkPairs = pkPairs.Replace(pkPair, string.Format("{0}:{1}", pk, long.Parse(pkPair.Split(":")[1]) + value));

			return pkPairs;
		}
	}
}