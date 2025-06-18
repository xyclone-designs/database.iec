using Database.IEC.Inputs.CSVs;

using SQLite;

using System.Collections.Generic;
using System.IO;

using XycloneDesigns.Apis.IEC.Tables;
using XycloneDesigns.Apis.General.Tables;

namespace Database.IEC
{
    internal partial class Program
    {
		public class Parameters<TRow> : Parameters where TRow : CSVRow
		{
			public List<TRow> Rows { get; set; } = [];
		}
		public class Parameters
		{
			public List<Ballot>? Ballots { get; set; }
			public List<Ballot>? BallotsElectoralEvent { get; set; }
			public ElectoralEvent? ElectoralEvent { get; set; }
			public List<ElectoralEvent>? ElectoralEvents { get; set; }
			public List<Party>? Parties { get; set; }
			public List<Municipality>? Municipalities { get; set; }
			public List<Province>? Provinces { get; set; }
			public List<VotingDistrict>? VotingDistricts { get; set; }
			public List<Ward>? Wards { get; set; }

			public SQLiteConnection? SqliteConnection { get; set; }
			public SQLiteConnection? SqliteConnectionMunicipalities { get; set; }
			public SQLiteConnection? SqliteConnectionProvinces { get; set; }

			public StreamWriter? Logger { get; set; }
			public string? LoggerTag { get; set; }
		}
	}
}