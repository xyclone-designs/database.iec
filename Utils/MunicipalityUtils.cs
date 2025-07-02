
namespace Database.IEC.Utils
{
	public class MunicipalityUtils
	{
		public static int? ProvinceFromMunicipality(string? geocode, string? Name, string? Namelong)
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
				true when Name is null => new int?(),
				true when Name.Contains("Eastern Cape") => 1,
				true when Name.Contains("Free State") => 2,
				true when Name.Contains("Gauteng") => 3,
				true when Name.Contains("KwaZulu-Natal") => 4,
				true when Name.Contains("Limpopo") => 5,
				true when Name.Contains("Mpumalanga") => 6,
				true when Name.Contains("Northern Cape") => 7,
				true when Name.Contains("North West") => 8,
				true when Name.Contains("Western Cape") => 9,

				_ => new int?()
			};

			province ??= true switch
			{
				true when Namelong is null => new int?(),
				true when Namelong.Contains("Eastern Cape") => 1,
				true when Namelong.Contains("Free State") => 2,
				true when Namelong.Contains("Gauteng") => 3,
				true when Namelong.Contains("KwaZulu-Natal") => 4,
				true when Namelong.Contains("Limpopo") => 5,
				true when Namelong.Contains("Mpumalanga") => 6,
				true when Namelong.Contains("Northern Cape") => 7,
				true when Namelong.Contains("North West") => 8,
				true when Namelong.Contains("Western Cape") => 9,

				_ => new int?()
			};

			return province;
		}
	}
}