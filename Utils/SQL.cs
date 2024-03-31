using DataProcessor.Tables;

using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Linq;

using SQLite;
using SQLitePCL;

using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace DataProcessor.Utils
{
    public class SQL
    {
        public static bool ReportProvince(int? provincePK, SQLiteConnection sqliteConnection, StreamWriter? streamWriterErrors)
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
        public static bool ReportMunicipality(string? municipalityGeoCodeOrName, SQLiteConnection sqliteConnection, StreamWriter? streamWriterErrors)
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
        public static bool ReportParty(string? partyName, SQLiteConnection sqliteConnection, StreamWriter? streamWriterErrors)
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
        public static bool ReportVotingDistrict(string votingDistrictId, SQLiteConnection sqliteConnection, StreamWriter? streamWriterErrors)
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
        public static bool ReportWard(string wardId, SQLiteConnection sqliteConnection, StreamWriter? streamWriterErrors)
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