using Database.IEC.Inputs.CSVs;
using Database.IEC.Utils;

using SQLite;

using System.Collections.Generic;
using System.Linq;

using XycloneDesigns.Apis.IEC.Tables;

namespace System.IO.Compression
{
	public static class ZipArchiveExtensions
	{
		public static void ReadDataAllocations(this ZipArchive zipArchive, SQLiteConnection sqliteConnection, StreamWriter log)
        {
			using Stream stream = zipArchive.Entries.First(entry => entry.FullName.EndsWith("_ELECTORALEVENTS.csv")).Open();
			using StreamReader streamReader = new(stream);
			streamReader.ReadLine();

			IEnumerable<string[]> Read()
            {
                while (streamReader.ReadLine()?.Split(',') is string[] rows)
                    yield return rows;
            }
			IEnumerable<ElectoralEvent> ElectoralEvents()
            {
				foreach (IGrouping<string, string[]> grouped in Read().GroupBy(_ => _[0]))
				{
					// Name,abbr,date,designation,party,allocation

					IEnumerable<string> allocations = grouped
						.Where(allocationline =>
						{
							return
								string.IsNullOrWhiteSpace(allocationline[3]) is false &&
								string.IsNullOrWhiteSpace(allocationline[4]) is false &&
								string.IsNullOrWhiteSpace(allocationline[5]) is false;
						})
						.Select(allocationline =>
						{
                            Party party = ((IEnumerable<Party>)sqliteConnection.Table<Party>())
								.Single(_party => string.Equals(_party.Name, allocationline[4], StringComparison.OrdinalIgnoreCase));

							return string.Format("{0}:{1}:{2}", party.Pk, allocationline[3], allocationline[5]);
						
                        });

					yield return new ElectoralEvent
					{
						Pk = CSVRow.Utils.RowToElectoralEvent(grouped.First()[0]) ?? throw new Exception(),
						Abbr = grouped.First()[1],
						Date = grouped.First()[2],
						Name = grouped.First()[0],
						Type = ElectoralEvent.ToType(grouped.Key),
						List_PkParty_Designation_NationalAllocation = ElectoralEvent.IsNational(grouped.Key) ? string.Join(',', allocations) : null,
						List_PkParty_IdProvince_ProvincialAllocation = ElectoralEvent.IsProvincial(grouped.Key) ? string.Join(',', allocations) : null,
						List_PkParty_IdProvince_RegionalAllocation = ElectoralEvent.IsRegional(grouped.Key) ? string.Join(',', allocations) : null,
					};
				}
			}

			sqliteConnection.InsertAll(ElectoralEvents());
		}
		public static void ReadDataMunicipalities(this ZipArchive zipArchive, SQLiteConnection sqliteConnection, StreamWriter log)
		{
			IEnumerable<Municipality> geographies = MunicipalityUtils.ReadFromGeography(zipArchive, log);
			IEnumerable<Municipality> members = MunicipalityUtils.ReadFromMembers(zipArchive, log);

            sqliteConnection.InsertAll(Enumerable.Empty<Municipality>()
                .Concat(geographies)
                .Concat(members)
				.DistinctBy(municipality => municipality.GeoCode)
                .OrderBy(municipality => municipality.GeoCode)
                .ThenBy(municipality => municipality.Name)
                .ThenBy(municipality => municipality.NameLong));
        }
        public static void ReadDataProvinces(this ZipArchive zipArchive, SQLiteConnection sqliteConnection, StreamWriter log)
        {
            using Stream stream = zipArchive.Entries.First(entry => entry.FullName.EndsWith("_PROVINCES.csv")).Open();
            using StreamReader streamReader = new(stream);
            streamReader.ReadLine();

			IEnumerable<string[]> Rows()
			{
				while (streamReader.ReadLine()?.Split(',') is string[] columns && string.IsNullOrWhiteSpace(columns[0]) is false)
					yield return columns;
			}

			sqliteConnection.InsertAll(Rows().Select(row => new Province
			{
				// Id,Name,capital,population,squareKms,urlWebsite

				Id = row[0],
				Name = row[1],
				Capital = row[2],
				Population = int.Parse(row[3]),
				SquareKms = int.Parse(row[4]),
				UrlWebsite = row[5],
			}));
        }
        public static void ReadDataParties(this ZipArchive zipArchive, SQLiteConnection sqliteConnection, StreamWriter log)
        {
			Console.WriteLine("ReadDataParties");

			List<Party> parties = [];

			using Stream stream = zipArchive.Entries.First(entry => entry.FullName.EndsWith("_PARTIES.csv")).Open();
            using StreamReader streamReader = new(stream);
            streamReader.ReadLine();

			for (int line = 2; streamReader.ReadLine()?.Split(',') is string[] columns; line++)
			{
				Console.WriteLine("[{0}]: {1} - {2}", line, columns[0], columns[1]);

				if (columns.Length != 6) throw new ArgumentException("Too many columns");
				
				parties.Add(new Party
				{
					// Name,abbr,color,headquarters,urlLogo,urlWebsite

					Name = string.Equals("{placeholder}", columns[0]) is false ? columns[0] : null,
					Abbr = string.Equals("{placeholder}", columns[1]) is false ? columns[1] : null,
					Color = string.Equals("{placeholder}", columns[2]) is false ? columns[2] : null,
					Headquarters = string.Equals("{placeholder}", columns[3]) is false ? columns[3] : null,
					UrlLogo = string.Equals("{placeholder}", columns[4]) is false ? columns[4] : null,
					UrlWebsite = string.Equals("{placeholder}", columns[5]) is false ? columns[5] : null,
				});
			}

			sqliteConnection.InsertAll(parties);
		}
    }
}
