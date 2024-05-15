using DataProcessor.CSVs;
using DataProcessor.Tables;

using Newtonsoft.Json.Linq;

using SQLite;

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;

namespace DataProcessor
{
	internal partial class Program
    {
		static void ReadDataAllocations(SQLiteConnection sqliteConnection, ZipArchive data, StreamWriter log)
        {
            IEnumerable<string[]> Read()
            {
                using Stream stream = data.Entries.First(entry => entry.FullName.EndsWith("_ELECTORALEVENTS.csv")).Open();
                using StreamReader streamReader = new(stream);
                streamReader.ReadLine();
                while (streamReader.ReadLine()?.Split(',') is string[] rows)
                    yield return rows;
            }

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
                            .First(_party => string.Equals(_party.name, allocationline[3], StringComparison.OrdinalIgnoreCase));

                        return string.Format("{0}:{1}:{2}", party.pk, allocationline[2], allocationline[4]);
                    });

                sqliteConnection.Insert(new ElectoralEvent
                {
                    pk = CSVRow.Utils.RowToElectoralEvent(grouped.First()[0]) ?? throw new Exception(),
                    date = grouped.First()[1],
                    type = ElectoralEvent.Type(grouped.Key),
                    list_pkParty_designation_nationalAllocation = ElectoralEvent.IsNational(grouped.Key) ? string.Join(',', allocations) : null,
                    list_pkParty_idProvince_provincialAllocation = ElectoralEvent.IsProvincial(grouped.Key) ? string.Join(',', allocations) : null,
                });
            }
        }
        static void ReadDataMunicipalities(SQLiteConnection sqliteConnection, ZipArchive data, StreamWriter log)
        {
            using HttpClient client = new();

            string Geography()
            {
                string? geography = null;
                string uri = "https://municipalmoney.gov.za/api/geography/geography/";

                try
                {
                    throw new Exception();
                    log.WriteLine("Querying municipalities at '{0}'", uri);
                    Console.WriteLine("Querying municipalities at '{0}'", uri);
                    using HttpRequestMessage request = new(HttpMethod.Get, uri);
                    using HttpResponseMessage response = client.Send(request);
                    if (response.IsSuccessStatusCode is false)
                        throw new HttpRequestException(string.Format("Querying municipalities at '{0}' unsuccessfull. Code: {1}", uri, response.StatusCode));
                    geography = response.Content
                        .ReadAsStringAsync()
                        .GetAwaiter()
                        .GetResult(); log.WriteLine("Querying municipalities at '{0}' successfull", uri);
                    log.WriteLine("Querying municipalities at '{0}' successfull", uri);
                    Console.WriteLine("Querying municipalities at '{0}' successfull", uri);
                }
                catch (Exception ex) { log.WriteLine(ex.Message); Console.WriteLine(ex.Message); }

                if (geography is not null)
                    return geography;

                log.WriteLine("Using static data for municipalities at '{0}'", uri);
                Console.WriteLine("Using static data for municipalities at '{0}'", uri);
                using Stream stream = data.Entries.First(entry => entry.FullName.EndsWith("_MUNICIPALITIES_GEOGRAPHY.json")).Open();
                using StreamReader streamReader = new(stream);
                geography = streamReader.ReadToEnd();

                return geography;
            }
            string Members()
            {
                string? members = null;
                string uri = "https://municipaldata.treasury.gov.za/api/cubes/municipalities/members/municipality";

                try
                {
                    throw new Exception();
                    log.WriteLine("Querying municipalities at '{0}'", uri);
                    Console.WriteLine("Querying municipalities at '{0}'", uri);
                    using HttpRequestMessage request = new(HttpMethod.Get, uri);
                    using HttpResponseMessage response = client.Send(request);
                    if (response.IsSuccessStatusCode is false)
                        throw new HttpRequestException(string.Format("Querying municipalities at '{0}' unsuccessfull. Code: {1}", uri, response.StatusCode));
                    members = response.Content
                        .ReadAsStringAsync()
                        .GetAwaiter()
                        .GetResult();
                    log.WriteLine("Querying municipalities at '{0}' successfull", uri);
                    Console.WriteLine("Querying municipalities at '{0}' successfull", uri);
                }
                catch (Exception ex) { log.WriteLine(ex.Message); Console.WriteLine(ex.Message); }

                if (members is not null)
                    return members;

                log.WriteLine("Using static data for municipalities at '{0}'", uri);
                Console.WriteLine("Using static data for municipalities at '{0}'", uri);
                using Stream stream = data.Entries.First(entry => entry.FullName.EndsWith("_MUNICIPALITIES_MEMBERS.json")).Open();
                using StreamReader streamReader = new(stream);
                members = streamReader.ReadToEnd();

                return members;
            }
            Province? ProvinceFor(Municipality municipality)
            {
                return ((IEnumerable<Province>)sqliteConnection.Table<Province>())
                        .FirstOrDefault(_province =>
                        {
                            return
                            (
                                string.IsNullOrWhiteSpace(_province.id) is false &&
                                string.IsNullOrWhiteSpace(municipality.geoCode) is false &&
                                municipality.geoCode.Contains(_province.id, StringComparison.OrdinalIgnoreCase)
                            )
                            ||
                            (
                                string.IsNullOrWhiteSpace(_province.name) is false &&
                                string.IsNullOrWhiteSpace(municipality.nameLong) is false &&
                                municipality.nameLong.Contains(_province.name, StringComparison.OrdinalIgnoreCase)
                            );
                        });
            }

            /*
                
                 [
                    {
                        "bbox": [],
                        "is_disestablished": true,
                        "geo_level": "municipality",
                        "geo_code": "EC103",
                        "name": "Ikwezi",
                        "long_name": "Ikwezi, Eastern Cape",
                        "square_kms": 4562.73,
                        "parent_level": "district",
                        "parent_code": "DC10",
                        "province_name": "Eastern Cape",
                        "province_code": "EC",
                        "category": "B",
                        "miif_category": "B3",
                        "population": 10537,
                        "postal_address_1": "P.O BOX 12",
                        "postal_address_2": "JANSENVILLE",
                        "postal_address_3": "6265",
                        "street_address_1": "34 Main Street",
                        "street_address_2": "Jansenville",
                        "street_address_3": "6265",
                        "street_address_4": null,
                        "phone_number": "049 836 0021",
                        "fax_number": "049 836 0105",
                        "url": "http://www.ikwezimunicipality.co.za"
                    }
                 ]

                 */

            foreach (JToken jtoken in JArray.Parse(Geography()))
                try
                {
                    JObject jobject = JObject.Parse(jtoken.ToString());
                    Municipality municipality = sqliteConnection
                        .FindOrCreateAndAdd<Municipality>(_ => _.geoCode == jobject.Value<string>("geo_code"), _ => { });

                    municipality.isDisestablished ??= jobject.Value<bool>("is_disestablished");
                    municipality.name ??= jobject.Value<string>("name");
                    municipality.nameLong ??= jobject.Value<string>("long_name");
                    municipality.squareKms ??= jobject.Value<decimal>("square_kms");
                    municipality.geoLevel ??= jobject.Value<string>("geo_level");
                    municipality.geoCode ??= jobject.Value<string>("geo_code");
                    municipality.category ??= jobject.Value<string>("category");
                    municipality.miifCategory ??= jobject.Value<string>("miif_category");
                    municipality.population ??= jobject.Value<int>("population");
                    municipality.addressPostal ??= string.Format(
                        "{0}\n{1}\n{2}",
                        jobject.Value<string>("postal_address_1"),
                        jobject.Value<string>("postal_address_2"),
                        jobject.Value<string>("postal_address_3"));
                    municipality.addressStreet ??= string.Format(
                        "{0}\n{1}\n{2}\n{3}",
                        jobject.Value<string>("street_address_1"),
                        jobject.Value<string>("street_address_2"),
                        jobject.Value<string>("street_address_3"),
                        jobject.Value<string>("street_address_4"));
                    municipality.numberPhone ??= jobject.Value<string>("phone_number");
                    municipality.numberFax ??= jobject.Value<string>("fax_number");
                    municipality.urlWebsite ??= jobject.Value<string>("url");

                    municipality.nameLong?.Replace("Kwazulu-Natal", "KwaZulu-Natal");
                    if (ProvinceFor(municipality) is Province province)
                        municipality.pkProvince = province.pk;

                    sqliteConnection.Update(municipality);
                }
                catch (Exception ex) { log.WriteLine(ex.Message); }

            /*

                     {
                        "total_member_count": 292,
                        "data": [
                            {
                                "municipality.demarcation_code": "BUF",
                                "municipality.name": "Buffalo City",
                                "municipality.long_name": "Buffalo City, Eastern Cape",
                                "municipality.parent_code": "EC",
                                "municipality.province_name": "Eastern Cape",
                                "municipality.province_code": "EC",
                                "municipality.category": "A",
                                "municipality.miif_category": "A",
                                "municipality.postal_address_1": "PO BOX 134",
                                "municipality.postal_address_2": "EAST LONDON",
                                "municipality.postal_address_3": "5200",
                                "municipality.street_address_1": "Trust Bank Centre",
                                "municipality.street_address_2": "C/O Oxford & North Street",
                                "municipality.street_address_3": "East London",
                                "municipality.street_address_4": "5200",
                                "municipality.phone_number": "043 705 2000",
                                "municipality.fax_number": "043 743 8568",
                                "municipality.url": "http://Www.Buffalocity.Gov.Za"
                            },
                        ]
                     }

                     */

            foreach (JToken jtoken in JObject.Parse(Members()).Value<JArray>("data")!)
                try
                {
                    JObject jobject = JObject.Parse(jtoken.ToString());
                    Municipality municipality = sqliteConnection
                        .FindOrCreateAndAdd<Municipality>(_ => _.geoCode == jobject.Value<string>("municipality.demarcation_code"), _ => { });

                    municipality.isDisestablished = false;
                    municipality.name ??= jobject.Value<string>("municipality.name");
                    municipality.nameLong ??= jobject.Value<string>("municipality.long_name");
                    municipality.geoCode ??= jobject.Value<string>("municipality.demarcation_code");
                    municipality.category ??= jobject.Value<string>("municipality.category");
                    municipality.miifCategory ??= jobject.Value<string>("municipality.miif_category");
                    municipality.addressPostal ??= string.Format(
                        "{0}\n{1}\n{2}",
                        jobject.Value<string>("municipality.postal_address_1"),
                        jobject.Value<string>("municipality.postal_address_2"),
                        jobject.Value<string>("municipality.postal_address_3"));
                    municipality.addressStreet ??= string.Format(
                        "{0}\n{1}\n{2}\n{3}",
                        jobject.Value<string>("municipality.street_address_1"),
                        jobject.Value<string>("municipality.street_address_2"),
                        jobject.Value<string>("municipality.street_address_3"),
                        jobject.Value<string>("municipality.street_address_4"));
                    municipality.numberPhone ??= jobject.Value<string>("municipality.phone_number");
                    municipality.numberFax ??= jobject.Value<string>("municipality.fax_number");
                    municipality.urlWebsite ??= jobject.Value<string>("municipality.url");

                    municipality.nameLong?.Replace("Kwazulu-Natal", "KwaZulu-Natal");
                    if (ProvinceFor(municipality) is Province province)
                        municipality.pkProvince = province.pk;

                    sqliteConnection.Update(municipality);
                }
                catch (Exception ex) { log.WriteLine(ex.Message); }
        }
        static void ReadDataProvinces(SQLiteConnection sqliteConnection, ZipArchive data, StreamWriter log)
        {
            using Stream stream = data.Entries.First(entry => entry.FullName.EndsWith("_PROVINCES.csv")).Open();
            using StreamReader streamReader = new(stream);
            streamReader.ReadLine();
            while (streamReader.ReadLine()?.Split(',') is string[] rows)
                sqliteConnection.Insert(new Province
                {
                    // id,name,population,squareKms,urlWebsite

                    id = rows[0],
                    name = rows[1],
                    population = int.Parse(rows[2]),
                    squareKms = int.Parse(rows[3]),
                    urlWebsite = rows[4],
                });
        }
        static void ReadDataParties(SQLiteConnection sqliteConnection, ZipArchive data, StreamWriter log)
        {
            using Stream stream = data.Entries.First(entry => entry.FullName.EndsWith("_PARTIES.csv")).Open();
            using StreamReader streamReader = new(stream);
            streamReader.ReadLine();
            while (streamReader.ReadLine()?.Split(',') is string[] rows && string.IsNullOrWhiteSpace(rows[0]) is false)
                sqliteConnection.Insert(new Party
                {
                    // name,abbr,color,urlLogo

                    name = rows[0],
                    abbr = rows[1],
                    color = rows[2],
                    urlLogo = rows[3],
                });
        }
    }
}
