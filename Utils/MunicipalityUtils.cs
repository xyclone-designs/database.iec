using DataProcessor.Tables;

using Newtonsoft.Json.Linq;

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace DataProcessor.Utils
{
	public class MunicipalityUtils
	{
		public static int? ProvinceFromMunicipality(string? geocode, string? name, string? namelong)
		{
			int? province = null;

			province ??= true switch
			{
				true when geocode is null => new int?(),
				true when geocode.Contains("EC") => 1,
				true when geocode.Contains("FS") => 2,
				true when geocode.Contains("GT") => 3,
				true when geocode.Contains("KZN") => 4,
				true when geocode.Contains("LIM") => 5,
				true when geocode.Contains("MP") => 6,
				true when geocode.Contains("NC") => 7,
				true when geocode.Contains("NW") => 8,
				true when geocode.Contains("WC") => 9,

				_ => new int?()
			};

			province ??= true switch
			{
				true when name is null => new int?(),
				true when name.Contains("Eastern Cape") => 1,
				true when name.Contains("Free State") => 2,
				true when name.Contains("Gauteng") => 3,
				true when name.Contains("KwaZulu-Natal") => 4,
				true when name.Contains("Limpopo") => 5,
				true when name.Contains("Mpumalanga") => 6,
				true when name.Contains("Northern Cape") => 7,
				true when name.Contains("North West") => 8,
				true when name.Contains("Western Cape") => 9,

				_ => new int?()
			};

			province ??= true switch
			{
				true when namelong is null => new int?(),
				true when namelong.Contains("Eastern Cape") => 1,
				true when namelong.Contains("Free State") => 2,
				true when namelong.Contains("Gauteng") => 3,
				true when namelong.Contains("KwaZulu-Natal") => 4,
				true when namelong.Contains("Limpopo") => 5,
				true when namelong.Contains("Mpumalanga") => 6,
				true when namelong.Contains("Northern Cape") => 7,
				true when namelong.Contains("North West") => 8,
				true when namelong.Contains("Western Cape") => 9,

				_ => new int?()
			};

			return province;
		}

		public static async Task DownloadFromGeography(StreamWriter output, StreamWriter? log)
		{
			using HttpClient client = new();

			try
			{
				string uri = "https://municipalmoney.gov.za/api/geography/geography/";

				log?.WriteLine("Querying municipalities at '{0}'", uri);
				Console.WriteLine("Querying municipalities at '{0}'", uri);
				
				using HttpRequestMessage request = new(HttpMethod.Get, uri);
				using HttpResponseMessage response = await client.SendAsync(request);
				
				if (response.IsSuccessStatusCode is false)
					throw new HttpRequestException(string.Format("Querying municipalities at '{0}' unsuccessfull. Code: {1}", uri, response.StatusCode));

				string geography = await response.Content.ReadAsStringAsync();

				output.Write(geography);
				log?.WriteLine("Querying municipalities at '{0}' successfull", uri);
				Console.WriteLine("Querying municipalities at '{0}' successfull", uri);
			}
			catch (Exception ex) { log?.WriteLine(ex.Message); Console.WriteLine(ex.Message); }
		}
		public static async Task DownloadFromMembers(StreamWriter output, StreamWriter? log)
		{
			using HttpClient client = new();

			string uri = "https://municipaldata.treasury.gov.za/api/cubes/municipalities/members/municipality";

			try
			{
				log?.WriteLine("Querying municipalities at '{0}'", uri);
				Console.WriteLine("Querying municipalities at '{0}'", uri);

				using HttpRequestMessage request = new(HttpMethod.Get, uri);
				using HttpResponseMessage response = await client.SendAsync(request);

				if (response.IsSuccessStatusCode is false)
					throw new HttpRequestException(string.Format("Querying municipalities at '{0}' unsuccessfull. Code: {1}", uri, response.StatusCode));
				
				string members = await response.Content.ReadAsStringAsync();

				output.Write(members);
				log?.WriteLine("Querying municipalities at '{0}' successfull", uri);
				Console.WriteLine("Querying municipalities at '{0}' successfull", uri);
			}
			catch (Exception ex) { log?.WriteLine(ex.Message); Console.WriteLine(ex.Message); }
		}

		public static IEnumerable<Municipality> ReadFromGeography(string json, StreamWriter? log)
		{
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

			if (JArray.Parse(json) is JArray data)
				for (int index = 0; index < data.Count; index++)
				{
					JObject? jobject = default;
					try { jobject = JObject.Parse(data[index].ToString()); }
					catch (Exception ex) { log?.WriteLine("{0}: {1}", index, ex.Message); Console.WriteLine("{0}: {1}", data[index], ex.Message); }

					if (jobject is not null)
					{
						string?
							geocode = jobject.Value<string>("geo_code"),
							name = jobject.Value<string>("name"),
							namelong = jobject.Value<string>("long_name")?.Replace("Kwazulu-Natal", "KwaZulu-Natal");

						yield return new Municipality
						{
							name = name,
							nameLong = namelong,
							geoCode = geocode,
							pkProvince = ProvinceFromMunicipality(geocode, name, namelong),
							isDisestablished = jobject.Value<bool>("is_disestablished"),
							squareKms = jobject.Value<decimal>("square_kms"),
							geoLevel = jobject.Value<string>("geo_level"),
							category = jobject.Value<string>("category"),
							miifCategory = jobject.Value<string>("miif_category"),
							population = jobject.Value<int>("population"),
							addressPostal = string.Format(
								"{0}\n{1}\n{2}",
								jobject.Value<string>("postal_address_1"),
								jobject.Value<string>("postal_address_2"),
								jobject.Value<string>("postal_address_3")),
							addressStreet = string.Format(
								"{0}\n{1}\n{2}\n{3}",
								jobject.Value<string>("street_address_1"),
								jobject.Value<string>("street_address_2"),
								jobject.Value<string>("street_address_3"),
								jobject.Value<string>("street_address_4")),
							numberPhone = jobject.Value<string>("phone_number"),
							numberFax = jobject.Value<string>("fax_number"),
							urlWebsite = jobject.Value<string>("url"),
						};
					}
				}
		}
		public static IEnumerable<Municipality> ReadFromMembers(string json, StreamWriter? log)
		{
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

			if (JObject.Parse(json).Value<JArray>("data") is JArray data)
				for (int index = 0; index < data.Count; index++)
				{
					JObject? jobject = default;
					try { jobject = JObject.Parse(data[index].ToString()); } 
					catch (Exception ex) { log?.WriteLine("{0}: {1}", index, ex.Message); Console.WriteLine("{0}: {1}", data[index], ex.Message); }

					if (jobject is not null)
					{
						string?
							geocode = jobject.Value<string>("municipality.demarcation_code"),
							name = jobject.Value<string>("municipality.name"),
							namelong = jobject.Value<string>("municipality.long_name")?.Replace("Kwazulu-Natal", "KwaZulu-Natal");

						yield return new Municipality
						{
							name = name,
							nameLong = namelong,
							geoCode = geocode,
							pkProvince = ProvinceFromMunicipality(geocode, name, namelong),
							isDisestablished = false,
							category = jobject.Value<string>("municipality.category"),
							miifCategory = jobject.Value<string>("municipality.miif_category"),
							addressPostal = string.Format(
								"{0}\n{1}\n{2}",
								jobject.Value<string>("municipality.postal_address_1"),
								jobject.Value<string>("municipality.postal_address_2"),
								jobject.Value<string>("municipality.postal_address_3")),
							addressStreet = string.Format(
								"{0}\n{1}\n{2}\n{3}",
								jobject.Value<string>("municipality.street_address_1"),
								jobject.Value<string>("municipality.street_address_2"),
								jobject.Value<string>("municipality.street_address_3"),
								jobject.Value<string>("municipality.street_address_4")),
							numberPhone = jobject.Value<string>("municipality.phone_number"),
							numberFax = jobject.Value<string>("municipality.fax_number"),
							urlWebsite = jobject.Value<string>("municipality.url"),
						};
					}
				}
		}

		public static IEnumerable<Municipality> ReadFromGeography(ZipArchive zipArchive, StreamWriter? log)
		{
			using Stream stream = zipArchive.Entries.First(entry => entry.FullName.EndsWith("_MUNICIPALITIES_GEOGRAPHY.json")).Open();
			using StreamReader streamReader = new(stream);
			string geography = streamReader.ReadToEnd();

			return ReadFromGeography(geography, log);
		}
		public static IEnumerable<Municipality> ReadFromMembers(ZipArchive zipArchive, StreamWriter? log)
		{
			using Stream stream = zipArchive.Entries.First(entry => entry.FullName.EndsWith("_MUNICIPALITIES_MEMBERS.json")).Open();
			using StreamReader streamReader = new(stream);
			string members = streamReader.ReadToEnd();

			return ReadFromMembers(members, log);
		}
	}
}