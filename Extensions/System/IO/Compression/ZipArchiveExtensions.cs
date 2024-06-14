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
				// id,name,population,squareKms,urlWebsite

				id = row[0],
				name = row[1],
				population = int.Parse(row[2]),
				squareKms = int.Parse(row[3]),
				urlWebsite = row[4],
			}));
        }
        public static void ReadDataParties(this ZipArchive zipArchive, SQLiteConnection sqliteConnection, StreamWriter log)
        {
            using Stream stream = zipArchive.Entries.First(entry => entry.FullName.EndsWith("_PARTIES.csv")).Open();
            using StreamReader streamReader = new(stream);
            streamReader.ReadLine();

            IEnumerable<string[]> Rows()
            {
                while (streamReader.ReadLine()?.Split(',') is string[] columns && string.IsNullOrWhiteSpace(columns[0]) is false)
                    yield return columns;
			}

            sqliteConnection.InsertAll(Rows().Select(row => new Party
            {
                // name,abbr,color,urlLogo

                name = row[0],
                abbr = row[1],
                color = row[2],
                urlLogo = row[3],
            }));
        }
    }
}
