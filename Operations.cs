using DataProcessor.CSVs;
using DataProcessor.Tables;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace DataProcessor
{
    public static class Operations
    {
        readonly static List<Municipality> Municipalities = [];
        readonly static List<Party> Parties = [];
        readonly static List<VotingDistrict> VotingDistricts = [];
        readonly static List<Ward> Wards = [];

        public static void Log<TTable, TCSVRow>(TCSVRow row, StreamWriter writer)
            where TTable : ElectionsItem
            where TCSVRow : CSVRow
        {
            void LogMunicipality(CSVRow row)
            {
                if (row.MunicipalityName is null && row.MunicipalityGeo is null)
                    return;

                if (Municipalities.FirstOrDefault(_ => _.geoCode == row.MunicipalityGeo) is Municipality municipalityGeo)
                {
                    if (row.MunicipalityName is null || municipalityGeo.name is null || row.MunicipalityName == municipalityGeo.name)
                        return;

                    if (row.MunicipalityName.Contains(municipalityGeo.name))
                    {
                        writer.WriteLine("[{0}]: Municipality = {1} - {2} (Replaced)", row.LineNumber, row.MunicipalityGeo, row.MunicipalityName);
                        Console.WriteLine("[{0}]: Municipality = {1} - {2} (Replaced)", row.LineNumber, row.MunicipalityGeo, row.MunicipalityName);

                        Municipalities.Remove(municipalityGeo);
                        Municipalities.Add(new Municipality
                        {
                            geoCode = row.MunicipalityGeo,
                            name = row.MunicipalityName,
                        });
                    }
                }
                else
                {
                    writer.WriteLine("[{0}]: Municipality = {1} - {2}", row.LineNumber, row.MunicipalityGeo, row.MunicipalityName);
                    Console.WriteLine("[{0}]: Municipality = {1} - {2}", row.LineNumber, row.MunicipalityGeo, row.MunicipalityName);
                    Municipalities.Add(new Municipality
                    {
                        geoCode = row.MunicipalityGeo,
                        name = row.MunicipalityName,
                    });
                }
            }
            void LogParty(CSVRow row)
            {
                if (row.PartyName is null)
                    return;

                if (Parties.Any(_party => 
                {
                    return _party.name == row.PartyName;

                }) is false)
                {
                    writer.WriteLine("[{0}]: Party = {1}", row.LineNumber, row.PartyName);
                    Console.WriteLine("[{0}]: Party = {1}", row.LineNumber, row.PartyName);

                    Parties.Add(new Party
                    {
                        name = row.PartyName,
                    });
                }
            }
            void LogVotingDistrict(CSVRow row)
            {
                if (row.VotingDistrictId is null)
                    return;

                if (VotingDistricts.Any(_votingdistrict =>
                {
                    return _votingdistrict.id == row.VotingDistrictId;

                }) is false)
                {
                    writer.WriteLine("[{0}]: VotingDistrict = {1}", row.LineNumber, row.VotingDistrictId);
                    Console.WriteLine("[{0}]: VotingDistrict = {1}", row.LineNumber, row.VotingDistrictId);

                    VotingDistricts.Add(new VotingDistrict
                    {
                        id = row.VotingDistrictId,
                    });
                }
            }
            void LogWard(CSVRow row)
            {
                if (true switch
                {
                    true when row is CSVRowLGE csvrowlge => csvrowlge.WardId,
                    true when row is CSVRowNPE2 csvrownpe2 => csvrownpe2.WardId,

                    _ => null

                } is string ward && Wards.Any(_ward =>
                {
                    return _ward.id == ward;

                }) is false)
                {
                    writer.WriteLine("[{0}]: Ward = {1}", row.LineNumber, ward);
                    Console.WriteLine("[{0}]: Ward = {1}", row.LineNumber, ward);

                    Wards.Add(new Ward
                    {
                        id = ward,
                    });
                }
            }

            switch (true)
            {
                case true when typeof(TTable) == typeof(Municipality):
                    LogMunicipality(row);
                    break;

                case true when typeof(TTable) == typeof(Party):
                    LogParty(row);
                    break;

                case true when typeof(TTable) == typeof(VotingDistrict):
                    LogVotingDistrict(row);
                    break;

                case true when typeof(TTable) == typeof(Ward) && typeof(TCSVRow) != typeof(CSVRowNPE1):
                    LogWard(row);
                    break;

                default: break;
            }
        }
        public static void LogFinal(StreamWriter writer)
        {
            writer.WriteLine();
            writer.WriteLine("Final");

            writer.WriteLine();
            writer.WriteLine("Municipalities");
            writer.WriteLine();
            foreach (string municipality in Municipalities.Select(_ => string.Format("{0} - {1}", _.geoCode, _.name)).Order())
                writer.WriteLine(municipality);

            writer.WriteLine();
            writer.WriteLine("Parties");
            writer.WriteLine();
            foreach (Party party in Parties.OrderBy(_ => _.name))
                writer.WriteLine(party.name);

            writer.WriteLine();
            writer.WriteLine("VotingDistricts");
            writer.WriteLine();
            foreach (VotingDistrict votingDistrict in VotingDistricts.OrderBy(_ => _.id))
                writer.WriteLine(votingDistrict.id);

            writer.WriteLine();
            writer.WriteLine("Wards");
            writer.WriteLine();
            foreach (Ward ward in Wards.OrderBy(_ => _.id))
                writer.WriteLine(ward.id);
        }
    }
}
