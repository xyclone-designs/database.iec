using DataProcessor.CSVs;
using DataProcessor.Tables;
using DataProcessor.Utils;

using SQLite;

using System.Collections.Generic;
using System.Linq;

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
					// electoralevent,date,designation,party,allocation

					IEnumerable<string> allocations = grouped
						.Where(allocationline =>
						{
							return
								string.IsNullOrWhiteSpace(allocationline[2]) is false &&
								string.IsNullOrWhiteSpace(allocationline[3]) is false &&
								string.IsNullOrWhiteSpace(allocationline[4]) is false;
						})
						.Select(allocationline =>
						{
                            Party party = ((IEnumerable<Party>)sqliteConnection.Table<Party>())
								.Single(_party => string.Equals(_party.name, allocationline[3], StringComparison.OrdinalIgnoreCase));

							return string.Format("{0}:{1}:{2}", party.pk, allocationline[2], allocationline[4]);
						
                        });

					yield return new ElectoralEvent
					{
						pk = CSVRow.Utils.RowToElectoralEvent(grouped.First()[0]) ?? throw new Exception(),
						date = grouped.First()[1],
						type = ElectoralEvent.Type(grouped.Key),
						list_pkParty_designation_nationalAllocation = ElectoralEvent.IsNational(grouped.Key) ? string.Join(',', allocations) : null,
						list_pkParty_idProvince_provincialAllocation = ElectoralEvent.IsProvincial(grouped.Key) ? string.Join(',', allocations) : null,
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
				.DistinctBy(municipality => municipality.geoCode)
                .OrderBy(municipality => municipality.geoCode)
                .ThenBy(municipality => municipality.name)
                .ThenBy(municipality => municipality.nameLong));
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
				// id,name,capital,population,squareKms,urlWebsite

				id = row[0],
				name = row[1],
				capital = row[2],
				population = int.Parse(row[3]),
				squareKms = int.Parse(row[4]),
				urlWebsite = row[5],
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
					// name,abbr,color,headquarters,urlLogo,urlWebsite

					name = string.Equals("{placeholder}", columns[0]) is false ? columns[0] : null,
					abbr = string.Equals("{placeholder}", columns[1]) is false ? columns[1] : null,
					color = string.Equals("{placeholder}", columns[2]) is false ? columns[2] : null,
					headquarters = string.Equals("{placeholder}", columns[3]) is false ? columns[3] : null,
					urlLogo = string.Equals("{placeholder}", columns[4]) is false ? columns[4] : null,
					urlWebsite = string.Equals("{placeholder}", columns[5]) is false ? columns[5] : null,
				});
			}

			sqliteConnection.InsertAll(parties);
		}
    }
}
