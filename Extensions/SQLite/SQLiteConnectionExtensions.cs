using Database.IEC.Inputs.CSVs;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using XycloneDesigns.Apis.General.Tables;
using XycloneDesigns.Apis.IEC.Tables;

namespace SQLite
{
    public static class SQLiteConnectionExtensions
    {
		public static void CSVNew<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter? log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			CSVNewParties(sqliteConnection, log, rows);
			CSVNewMunicipalities(sqliteConnection, log, rows);
			CSVNewVotingDistricts(sqliteConnection, log, rows);
			CSVNewWards(sqliteConnection, log, rows);
		}
		public static List<Party> CSVNewParties<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter? log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("Parties");
			Console.WriteLine("Parties - Retrieving...");
			List<Party> parties = rows
				.Select(row => row.PartyName)
				.OfType<string>()
				.Distinct()
				.Where(Name =>
				{
					return string.IsNullOrWhiteSpace(Name) is false && Name != "0" && sqliteConnection
						.Table<Party>()
						.Any(party => string.Equals(Name, party.Name, StringComparison.OrdinalIgnoreCase)) is false;

				}).Select(Name => new Party
				{
					Name = Name,

				}).ToList();

			
			Console.WriteLine("Parties - Inserting...");
			sqliteConnection.InsertAll(parties);

			if (log is not null)
			{
				log.WriteLine("Parties - Inserting Names");
				foreach (Party party in parties) 
					log.WriteLine(party.Name ?? "null");
			}

			return parties;
		}
		public static List<Municipality> CSVNewMunicipalities<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter? log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("Municipalities");
			Console.WriteLine("Municipalities - Retrieving...");

			List<Municipality> municipalities = [];
			List<Municipality> _municipalities = [.. sqliteConnection.Table<Municipality>()];

			List<TCSVRow> _rows = rows
				.DistinctBy(row => row.MunicipalityName)
				.Where(row => row.MunicipalityName is not null)
				.ToList();

			Console.WriteLine("Municipalities - Inserting...");

			for (int lower = 0, upper = Math.Min(lower + 99, _rows.Count); true; lower = upper + 1, upper = Math.Min(lower + 99, _rows.Count))
			{
				Console.WriteLine("Municipalities - Inserting... [{0} - {1}] / {2}", lower, upper, _rows.Count);
				List<Municipality> municipalities_new = 
					(typeof(TCSVRow) == typeof(NPE1999)
						? _rows[lower..upper].Where(row => _municipalities.Any(municipality => string.Equals(row.MunicipalityName, municipality.Name, StringComparison.OrdinalIgnoreCase)) is false)
						: _rows[lower..upper].Where(row =>
						{
							return _municipalities.Any(municipality =>
							{
								return
									string.Equals(row.MunicipalityGeo, municipality.GeoCode, StringComparison.OrdinalIgnoreCase) ||
									string.Equals(row.MunicipalityName, municipality.Name, StringComparison.OrdinalIgnoreCase);

							}) is false;

						})).Select(row => new Municipality
						{
							PkProvince = row.ProvincePk,
							GeoCode = row.MunicipalityGeo,
							Name = row.MunicipalityName,

						}).ToList();

				if (municipalities_new.Count > 0)
					municipalities.AddRange(municipalities_new);				

				if (upper == _rows.Count)
					break;
			}

			if (municipalities.Count > 0)
				sqliteConnection.InsertAll(municipalities);

			if (log is not null)
			{
				log.WriteLine("Municipalities - Inserting Geos & Names");
				foreach (Municipality municipality in municipalities)
					log.WriteLine("[{0}] - {1}", municipality.GeoCode ?? "_", municipality.Name ?? "null");
			}

			return municipalities;
		}
		public static List<VotingDistrict> CSVNewVotingDistricts<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter? log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("VotingDistricts");
			Console.WriteLine("VotingDistricts - Retrieving...");
			List<VotingDistrict> votingdistricts = rows
				.Select(row => row.VotingDistrictId)
				.OfType<string>()
				.Distinct()
				.Where(Id => sqliteConnection.Table<VotingDistrict>().Any(_ => _.Id == Id) is false)
				.Select(Id => new VotingDistrict
				{
					Id = Id,

				}).ToList();

			Console.WriteLine("VotingDistricts - Inserting...");
			sqliteConnection.InsertAll(votingdistricts);

			if (log is not null)
			{
				log.WriteLine("VotingDistricts - Inserting Ids");
				foreach (VotingDistrict votingdistrict in votingdistricts)
					log.WriteLine(votingdistrict.Id ?? "null");
			}

			return votingdistricts;
		}
		public static List<Ward> CSVNewWards<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter? log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("Wards");
			Console.WriteLine("Wards - Retrieving...");
			List<Ward> wards = rows
				.Select(row => true switch
				{
					true when row is CSVRowLGE csvrowlge => csvrowlge.WardId,
					true when row is CSVRowNPE2 csvrownpe2 => csvrownpe2.WardId,

					_ => null

				}).OfType<string>().Distinct().Where(Id =>
				{
					return sqliteConnection
						.Table<Ward>()
						.Any(ward => Id == ward.Id) is false;

				}).Select(Id => new Ward
				{
					Id = Id,

				}).ToList();

			Console.WriteLine("Wards - Inserting...");
			sqliteConnection.InsertAll(wards);
			
			if (log is not null)
			{
				log.WriteLine("Wards - Inserting Ids");
				foreach (Ward ward in wards)
					log.WriteLine(ward.Id ?? "null");
			}

			return wards;
		}

		public static bool ReportProvince(this SQLiteConnection sqliteConnection, int? provincePK, StreamWriter? streamWriterErrors)
		{
			Province? province = provincePK is null ? null : sqliteConnection.Find<Province>(provincePK);

			if (provincePK is null)
			{
				Console.WriteLine("PROVNICE Not  {0}", provincePK);
				streamWriterErrors?.WriteLine("\"{0}\" => \"placeholder\",", provincePK);
			}
			else Console.WriteLine("PRovince  {0}", provincePK);

			return province is not null;
		}
		public static bool ReportMunicipality(this SQLiteConnection sqliteConnection, string? municipalityGeoCodeOrName, StreamWriter? streamWriterErrors)
		{
			municipalityGeoCodeOrName = municipalityGeoCodeOrName?.Trim().Trim('"');
			Municipality? municipality = municipalityGeoCodeOrName is null ? null : sqliteConnection.Find<Municipality>(
				municipality =>
					(municipality.Name != null && municipality.Name.ToLower() == municipalityGeoCodeOrName.ToLower()) ||
					(municipality.GeoCode != null && municipality.GeoCode.ToLower() == municipalityGeoCodeOrName.ToLower())
			);

			if (municipality is null)
			{
				Console.WriteLine("Municipality Not  {0}", municipalityGeoCodeOrName);
				streamWriterErrors?.WriteLine("\"{0}\" => \"placeholder\",", municipalityGeoCodeOrName);
			}
			else Console.WriteLine("Municipality  {0}", municipalityGeoCodeOrName);

			return municipality is not null;
		}
		public static bool ReportParty(this SQLiteConnection sqliteConnection, string? partyName, StreamWriter? streamWriterErrors)
		{
			partyName = partyName?.Trim().Trim('"');
			Party? party = partyName is null ? null : sqliteConnection.Find<Party>(
				party => party.Name != null && party.Name.ToLower() == partyName.ToLower()
			);

			if (party is null)
			{
				Console.WriteLine("Party Not  {0}", partyName);
				//streamWriterErrors?.WriteLine("\"{0}\" => \"{1}\",", partyName, Titlelise(partyName));
			}
			else Console.WriteLine("Party {0}", partyName);

			return party is not null;
		}
		public static bool ReportVotingDistrict(this SQLiteConnection sqliteConnection, string? votingDistrictId, StreamWriter? streamWriterErrors)
		{
			VotingDistrict? votingDistrict = sqliteConnection.Find<VotingDistrict>(votingDistrict => votingDistrict.Id == votingDistrictId);

			if (votingDistrict is null)
			{
				Console.WriteLine("VotingDistrict Not  {0}", votingDistrictId);
				streamWriterErrors?.WriteLine("\"{0}\" => \"placeholder\",", votingDistrictId);
			}
			else Console.WriteLine("VotingDistrict  {0}", votingDistrictId);

			return votingDistrict is not null;
		}
		public static bool ReportWard(this SQLiteConnection sqliteConnection, string? wardId, StreamWriter? streamWriterErrors)
		{
			Ward? ward = sqliteConnection.Find<Ward>(ward => ward.Id == wardId);

			if (ward is null)
			{
				Console.WriteLine("Ward Not  {0}", wardId);
				streamWriterErrors?.WriteLine("\"{0}\" => \"placeholder\",", wardId);
			}
			else Console.WriteLine("Ward {0}", wardId);

			return ward is not null;
		}
	}
}
