using DataProcessor.CSVs;
using DataProcessor.Tables;

using SQLite;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataProcessor
{
	internal partial class Program
    {
        public static void CSVNew<TCSVRow>(SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
        {
            CSVNewParties(sqliteConnection, log, rows);
            CSVNewMunicipalities(sqliteConnection, log, rows);
            CSVNewVotingDistricts(sqliteConnection, log, rows);
            CSVNewWards(sqliteConnection, log, rows);
        }
        public static List<Party> CSVNewParties<TCSVRow>(SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
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
        public static List<Municipality> CSVNewMunicipalities<TCSVRow>(SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
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
        public static List<VotingDistrict> CSVNewVotingDistricts<TCSVRow>(SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
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
        public static List<Ward> CSVNewWards<TCSVRow>(SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVRow
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
    }
}
