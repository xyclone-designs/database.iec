using Database.IEC.Inputs.CSVs;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;

using XycloneDesigns.Apis.General.Tables;
using XycloneDesigns.Apis.IEC.Tables;

using _TableIEC = XycloneDesigns.Apis.IEC.Tables._Table;

namespace SQLite
{
    public static class SQLiteConnectionExtensions
    {
        public static T CreateAndAdd<T>(this SQLiteConnection sqliteConnection, T t) where T : _TableIEC, new()
        {
            sqliteConnection.Insert(t);

            return sqliteConnection.Table<T>().Last();
        }
        public static T FindOrCreateAndAdd<T>(this SQLiteConnection sqliteConnection, Expression<Func<T, bool>> predicate, Action<T> onCreate) where T : _TableIEC, new()
        {
            T? t = null;
            try { t = sqliteConnection.Find<T>(predicate); } catch (Exception) { }
            t ??= ((IEnumerable<T>)sqliteConnection.Table<T>()).FirstOrDefault(predicate.Compile());
            if (t is not null) return t;

            onCreate.Invoke(t = new T());
            sqliteConnection.Insert(t);

            return sqliteConnection.Table<T>().Last();
        }
        public static T FindOrCreateAndAdd<T>(this SQLiteConnection sqliteConnection, int? pk, Action<T> onCreate) where T : _TableIEC, new()
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
                onCreate.Invoke(t = new T() { Pk = pk.Value });
                sqliteConnection.Insert(t);
            }

            return t;
        }

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

			log?.WriteLine("Parties - Inserting Names");
			Console.WriteLine("Parties - Inserting...");
			sqliteConnection.InsertAll(parties);
			foreach (Party party in parties) log.WriteLine(party.Name ?? "null");

			return parties;
		}
		public static List<Municipality> CSVNewMunicipalities<TCSVRow>(this SQLiteConnection sqliteConnection, StreamWriter? log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
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
							.Any(municipality => string.Equals(row.MunicipalityName, municipality.Name, StringComparison.OrdinalIgnoreCase)) is false;

					}).Select(row => new Municipality
					{
						PkProvince = row.ProvincePk,
						GeoCode = row.MunicipalityGeo,
						Name = row.MunicipalityName,

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
									string.Equals(row.MunicipalityGeo, municipality.GeoCode, StringComparison.OrdinalIgnoreCase) ||
									string.Equals(row.MunicipalityName, municipality.Name, StringComparison.OrdinalIgnoreCase);

							}) is false;

					}).Select(row => new Municipality
					{
						PkProvince = row.ProvincePk,
						GeoCode = row.MunicipalityGeo,
						Name = row.MunicipalityName,

					}).ToList();

			log?.WriteLine("Municipalities - Inserting Geos & Names");
			Console.WriteLine("Municipalities - Inserting...");
			sqliteConnection.InsertAll(municipalities);
			foreach (Municipality municipality in municipalities) log.WriteLine("[{0}] - {1}", municipality.GeoCode ?? "_", municipality.Name ?? "null");

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

			log?.WriteLine("VotingDistricts - Inserting Ids");
			Console.WriteLine("VotingDistricts - Inserting...");
			sqliteConnection.InsertAll(votingdistricts);
			foreach (VotingDistrict votingdistrict in votingdistricts) log.WriteLine(votingdistrict.Id ?? "null");

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

			log?.WriteLine("Wards - Inserting Ids");
			Console.WriteLine("Wards - Inserting...");
			sqliteConnection.InsertAll(wards);
			foreach (Ward ward in wards) log.WriteLine(ward.Id ?? "null");

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
