using DataProcessor.CSVs;
using DataProcessor.Tables;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;

namespace SQLite
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

		public static void CSVNew<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			CSVNewParties(sqliteConnection, log, rows);
			CSVNewMunicipalities(sqliteConnection, log, rows);
			CSVNewVotingDistricts(sqliteConnection, log, rows);
			CSVNewWards(sqliteConnection, log, rows);
		}
		public static List<Party> CSVNewParties<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("Parties");
			Console.WriteLine("Parties - Retrieving...");
			List<Party> parties = rows
				.Select(row => row.PartyName)
				.OfType<string>()
				.Distinct()
				.Where(name =>
				{
					return string.IsNullOrWhiteSpace(name) is false && name != "0" && sqliteConnection
						.Table<Party>()
						.Any(party => string.Equals(name, party.name)) is false;

				}).Select(name => new Party
				{
					name = name,

				}).ToList();

			log.WriteLine("Parties - Inserting Names");
			Console.WriteLine("Parties - Inserting...");
			sqliteConnection.InsertAll(parties);
			foreach (Party party in parties) log.WriteLine(party.name ?? "null");

			return parties;
		}
		public static List<Municipality> CSVNewMunicipalities<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("Municipalities");
			Console.WriteLine("Municipalities - Retrieving...");

			List<Municipality> municipalities = typeof(TCSVRow) == typeof(NPE1999)
				? rows
					.DistinctBy(row => row.MunicipalityName)
					.Where(row =>
					{
						return row.MunicipalityName is not null && sqliteConnection
							.Table<Municipality>()
							.Any(municipality => string.Equals(row.MunicipalityName, municipality.name, StringComparison.OrdinalIgnoreCase)) is false;

					}).Select(row => new Municipality
					{
						pkProvince = row.ProvincePk,
						geoCode = row.MunicipalityGeo,
						name = row.MunicipalityName,

					}).ToList()
				: rows
					.DistinctBy(row => row.MunicipalityName)
					.Where(row =>
					{
						return row.MunicipalityName is not null && sqliteConnection
							.Table<Municipality>()
							.Any(municipality =>
							{
								return
									string.Equals(row.MunicipalityGeo, municipality.geoCode, StringComparison.OrdinalIgnoreCase) ||
									string.Equals(row.MunicipalityName, municipality.name, StringComparison.OrdinalIgnoreCase);

							}) is false;

					}).Select(row => new Municipality
					{
						pkProvince = row.ProvincePk,
						geoCode = row.MunicipalityGeo,
						name = row.MunicipalityName,

					}).ToList();

			log.WriteLine("Municipalities - Inserting Geos & Names");
			Console.WriteLine("Municipalities - Inserting...");
			sqliteConnection.InsertAll(municipalities);
			foreach (Municipality municipality in municipalities) log.WriteLine("[{0}] - {1}", municipality.geoCode ?? "_", municipality.name ?? "null");

			return municipalities;
		}
		public static List<VotingDistrict> CSVNewVotingDistricts<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("VotingDistricts");
			Console.WriteLine("VotingDistricts - Retrieving...");
			List<VotingDistrict> votingdistricts = rows
				.Select(row => row.VotingDistrictId)
				.OfType<string>()
				.Distinct()
				.Where(id => sqliteConnection.Table<VotingDistrict>().Any(_ => _.id == id) is false)
				.Select(id => new VotingDistrict
				{
					id = id,

				}).ToList();

			log.WriteLine("VotingDistricts - Inserting Ids");
			Console.WriteLine("VotingDistricts - Inserting...");
			sqliteConnection.InsertAll(votingdistricts);
			foreach (VotingDistrict votingdistrict in votingdistricts) log.WriteLine(votingdistrict.id ?? "null");

			return votingdistricts;
		}
		public static List<Ward> CSVNewWards<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
		{
			Console.WriteLine("Wards");
			Console.WriteLine("Wards - Retrieving...");
			List<Ward> wards = rows
				.Select(row => true switch
				{
					true when row is CSVRowLGE csvrowlge => csvrowlge.WardId,
					true when row is CSVRowNPE2 csvrownpe2 => csvrownpe2.WardId,

					_ => null

				}).OfType<string>().Distinct().Where(id =>
				{
					return sqliteConnection
						.Table<Ward>()
						.Any(ward => id == ward.id) is false;

				}).Select(id => new Ward
				{
					id = id,

				}).ToList();

			log.WriteLine("Wards - Inserting Ids");
			Console.WriteLine("Wards - Inserting...");
			sqliteConnection.InsertAll(wards);
			foreach (Ward ward in wards) log.WriteLine(ward.id ?? "null");

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
					(municipality.name != null && municipality.name.ToLower() == municipalityGeoCodeOrName.ToLower()) ||
					(municipality.geoCode != null && municipality.geoCode.ToLower() == municipalityGeoCodeOrName.ToLower())
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
				party => party.name != null && party.name.ToLower() == partyName.ToLower()
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
			VotingDistrict? votingDistrict = sqliteConnection.Find<VotingDistrict>(votingDistrict => votingDistrict.id == votingDistrictId);

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
			Ward? ward = sqliteConnection.Find<Ward>(ward => ward.id == wardId);

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
