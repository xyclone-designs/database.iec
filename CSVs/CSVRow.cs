
using DataProcessor.Tables;

namespace DataProcessor.CSVs
{
    public abstract class CSVRow
    {
        public CSVRow(string line)
        {
            Line = line;
        }

        public string? Line { get; set; }
        public int? LineNumber { get; set; }
        public string? ElectoralEvent { get; set; }
        public int ElectoralEventPk 
        {
            get 
            {
                return Utils.RowToElectoralEvent(ElectoralEvent ?? throw new Exception("No ElectoralEvent")) ?? throw new Exception("Errorneous ElectoralEvent");
            }
        }
        public string? MunicipalityGeo { get; set; }
        public string? MunicipalityName { get; set; }
        public int? PartyVotes { get; set; }
        public int? ProvincePk { get; set; }
        public string? PartyName { get; set; }
        public int? RegisteredVoters { get; set; }
        public int? SpoiltVotes { get; set; }
        public string? VotingDistrictId { get; set; }
        
        public virtual string? GetBallotType() { return null; }
        public virtual string? GetWardId() { return null; }
        public virtual int? GetTotalVotes() { return null; }
        
        public Ballot AsBallot(Action<Ballot>? oninit = null) 
        {
            Ballot ballot = GenerateBallot();

            oninit?.Invoke(ballot);

            return ballot;
        }

        protected virtual Ballot GenerateBallot()
        {
            return new Ballot
            {
                pkElectoralEvent = ElectoralEventPk,
                pkProvince = ProvincePk,

                votersRegistered = RegisteredVoters,
                votesSpoilt = SpoiltVotes,
            };
        }

        public static class Utils
        {
            public static string? Clean(string text)
            {
                if (text.Length == 1 && text == "_")
                    return null;
                
                if (text.Equals("null", StringComparison.OrdinalIgnoreCase))
                    return null;

                return text
                    .Trim(' ', '"')
                    .Replace("%", string.Empty)
                    .Replace("  ", " ")
                    .Replace("   ", " ")
                    .Replace("    ", " ");
            }
            public static string Titlelise(string title)
            {
                title = title.ToUpper();
                System.Text.StringBuilder stringBuilder = new();
                for (int index = 0, ff = -1; index < title.Length; index++)
                {
                    stringBuilder.Append(ff == -1 ? title[index] : char.ToLower(title[index]));
                    ff = title[index] == ' ' || title[index] == '/' || title[index] == '\\' || title[index] == '-' || title[index] == '['
                        ? -1
                        : 0;
                }

                title = stringBuilder.ToString()
                    .Replace("(sa)", "(SA)");

                if (title.EndsWith(" dc", StringComparison.OrdinalIgnoreCase))
                    title = string.Join("", title[0..^3]) + " DC";
                else if (title.EndsWith(" np", StringComparison.OrdinalIgnoreCase))
                    title = string.Join("", title[0..^3]) + " NP";
                else if (title.EndsWith(" sa", StringComparison.OrdinalIgnoreCase))
                    title = string.Join("", title[0..^3]) + " SA";

                return title;
            }
            public static string[] RowsFromLine(string line, int expectedRowCount)
            {
                string newline = "";

                // Straight up replace some stuff beacuase somebody thought it was clever to have names with commas in a CSV document
                line = line
                    .Replace("SUNRISE PARK, PROTEA CITY AND GREENSIDE RESIDENTS", "SUNRISE PARK PROTEA CITY AND GREENSIDE RESIDENTS")
                    .Replace("KINGSLEY HOSTEL,HALL NO.2", "KINGSLEY HOSTEL HALL NO.2")
                    .Replace("TENT AT MONTAGUE DRIVE, PORTLANDS", "TENT AT MONTAGUE DRIVE; PORTLANDS")
                    .Replace("TECHNICAL COLLEGE, WEST CAMPUS", "TECHNICAL COLLEGE; WEST CAMPUS")
                    .Replace("EXT 7 OPEN SPACE, MHLUZI (TENT)", "EXT 7 OPEN SPACE; MHLUZI (TENT)")
                    .Replace("CAPE ACADEMY FOR MATHS, SCIENCE & TECHNOLOGY", "CAPE ACADEMY FOR MATHS SCIENCE & TECHNOLOGY")
                    .Replace("LOTHAIR PRIMARY SCHOOL, SILINDILE", "LOTHAIR PRIMARY SCHOOL; SILINDILE")
                    .Replace("ALPHA STREET, SAAMSTAAN (TENT)", "ALPHA STREET; SAAMSTAAN (TENT)")
                    .Replace("MAHAMBEHLALA TENT , MCADHUKISO STREET", "MAHAMBEHLALA TENT; MCADHUKISO STREET")
                    .Replace("DENNISIG, PARK 5075 NEXT TO CHURCH OF TODAY (TENT)", "DENNISIG; PARK 5075 NEXT TO CHURCH OF TODAY (TENT)")
                    .Replace("VGK EAST CHURH HALL, ROBERTSON", "VGK EAST CHURH HALL; ROBERTSON")
                    .Replace("DEPT,ROADS & PUBLIC WORKS", "DEPT OF ROADS & PUBLIC WORKS")
                    .Replace("TENT VACANT SITE CNRS VINK, GANS & QUAIL ", "TENT VACANT SITE CNRS VINK GANS & QUAIL ")
                    .Replace("TENT (TSUTSUMANE, EXT 7 )", "TSUTSUMANE; EXT 7 (TENT)")
                    .Replace("HIMEVILLE, JABULANI HALL", "JABULANI HALL; HIMEVILLE")
                    .Replace("BURNING FIRE JESUS MINISTRIES, MAKHUSHANE", "BURNING FIRE JESUS MINISTRIES; MAKHUSHANE")
                    .Replace("TENT - EXT 24, PARK NEXT TO HOUSE 854", "TENT NEAR EXT 24")
                    .Replace("ALPHEU ELECTRICAL, BUILDING AND CIVIL CONSTRUCTION", "ALPHEU ELECTRICAL BUILDING AND CIVIL CONSTRUCTION")
                    .Replace("TENT@ 11 MAUSER STREET,IFAFI", "TENT AT 11 MAUSER STREET; IFAFI")
                    .Replace("\"OUT OF COUNTRY\",\"\",,", "\"OUT OF COUNTRY\", \"OOC\",_,")
                    .Replace("KINGSLEY HOSTEL HALL NO.2, BLOCK 4", "KINGSLEY HOSTEL HALL NO.2; BLOCK 4")
                    .Replace("MOPANI CAMP,,", "MOPANI CAMP,")
                    .Replace("(No Voting Station Name),0,0,0,", "(No Voting Station Name),_,0,0,0,")
                    .Replace(",,", ",_,");

                // Some columns are eg: "8877,23";
                for (int index = 0, active = 0; index < line.Length; index++)
                {
                    if (newline.LastOrDefault() == ',' && line[index] == ',')
                        newline += '_' + line[index];
                    else if (newline.LastOrDefault() == '"' && line[index] == '"')
                        newline += '_' + line[index];
                    else if (active == 0 && line[index] != '"')
                        newline += line[index];
                    else if (active == 0 && line[index] == '"')
                    {
                        active = 1;
                        newline += line[index];
                    }
                    else if (active == 1 && line[index] != '"')
                        newline += line[index] switch
                        {
                            ',' => "",

                            _ => line[index]
                        };
                    else if (active == 1 && line[index] == '"')
                    {
                        active = 0;
                        newline += line[index];
                    }
                }

                string[] rows = newline.Split(',');

                if (rows.Length != expectedRowCount && string.IsNullOrEmpty(rows.Last()))
                    rows = rows.Take(rows.Length - 1).ToArray();

                if (rows.Length != expectedRowCount)
                    throw new Exception(string.Format("Bad Row Count \n {0}", newline));

                return rows;
            }

            public static int? RowToElectoralEvent(string row)
            {
                return true switch
                {
                    true when row.Contains("1994") => true switch
                    {
                        true when row.Contains(Tables.ElectoralEvent.Types.National, StringComparison.OrdinalIgnoreCase) => 1,
                        true when row.Contains(Tables.ElectoralEvent.Types.Provincial, StringComparison.OrdinalIgnoreCase) => 2,

                        _ => throw new Exception("1994 ElectoralEvent with no type")
                    },

                    true when row.Contains("1999") => true switch
                    {
                        true when row.Contains(Tables.ElectoralEvent.Types.National, StringComparison.OrdinalIgnoreCase) => 3,
                        true when row.Contains(Tables.ElectoralEvent.Types.Provincial, StringComparison.OrdinalIgnoreCase) => 4,

                        _ => throw new Exception("1999 ElectoralEvent with no type")
                    },

                    true when row.Contains("2000") => 5,

                    true when row.Contains("2004") => true switch
                    {
                        true when row.Contains(Tables.ElectoralEvent.Types.National, StringComparison.OrdinalIgnoreCase) => 6,
                        true when row.Contains(Tables.ElectoralEvent.Types.Provincial, StringComparison.OrdinalIgnoreCase) => 7,

                        _ => throw new Exception("2004 ElectoralEvent with no type")
                    },

                    true when row.Contains("2006") => 8,

                    true when row.Contains("2009") => true switch
                    {
                        true when row.Contains(Tables.ElectoralEvent.Types.National, StringComparison.OrdinalIgnoreCase) => 9,
                        true when row.Contains(Tables.ElectoralEvent.Types.Provincial, StringComparison.OrdinalIgnoreCase) => 10,

                        _ => throw new Exception("2009 ElectoralEvent with no type")
                    },

                    true when row.Contains("2011") => 11,

                    true when row.Contains("2014") => true switch
                    {
                        true when row.Contains(Tables.ElectoralEvent.Types.National, StringComparison.OrdinalIgnoreCase) => 12,
                        true when row.Contains(Tables.ElectoralEvent.Types.Provincial, StringComparison.OrdinalIgnoreCase) => 13,

                        _ => throw new Exception("2014 ElectoralEvent with no type")
                    },

                    true when row.Contains("2016") => 14,

                    true when row.Contains("2019") => true switch
                    {
                        true when row.Contains(Tables.ElectoralEvent.Types.National, StringComparison.OrdinalIgnoreCase) => 15,
                        true when row.Contains(Tables.ElectoralEvent.Types.Provincial, StringComparison.OrdinalIgnoreCase) => 16,

                        _ => throw new Exception("2019 ElectoralEvent with no type")
                    },

                    true when row.Contains("2021") => 17,

                    _ => throw new Exception("ElectoralEvent with no type"),
                };
            }
            public static string? RowToBallotType(string row)
            {
                return Clean(row)?.ToLower() is not string ballottype ? null : ballottype switch
                {
                    "dma dc 60%" or "dma dc 60" => "municipal.dmadc60",
                    "dc 40%" or "dc 40" => "municipal.dc40",
                    _ => "municipal." + ballottype,
                };
            }
            public static DateTime? RowToDateTime(string row)
            {
                return Clean(row) is string _row && DateTime.TryParse(_row, out DateTime value) 
                    ? value 
                    : new DateTime?();
            }
            public static decimal? RowToDecimal(string row)
            {
                return Clean(row) is string _row && decimal.TryParse(_row, out decimal value)
                    ? value
                    : new decimal?();
            }
            public static int? RowToInt(string row)
            {
                return Clean(row) is string _row && int.TryParse(_row, out int value)
                    ? value
                    : new int?();
            }
            public static string? RowToMunicipalityGeo(string _text)
            {
                /*
                 * Various Formats
                 * 
                 * EC05b1 - Umzimkulu [Umzimkulu]
                 * EC101 - CAMDEBOO [GRAAFF-REINET]
                 * BUF - Buffalo City
                 * BUF [Buffalo City]
                 * 
                 * **/

                if (_text is null || _text == "_")
                    return null;

                if (_text.Contains("out of country", StringComparison.OrdinalIgnoreCase) || _text.Equals("null", StringComparison.OrdinalIgnoreCase))
                    return "OOC";

                string[] textSplit = _text.Split('-', 2);

                if (textSplit.Length == 1 && textSplit[0].EndsWith(']'))
                    return Clean(textSplit[0].Split('[')[0]);

                _text = textSplit[0]
                    .Replace("KZDMA", "KZNDMA")
                    .Trim(' ', '"')
                    .ToUpper();

                _text = _text switch
                {
                    "CAPE TOWN" => "CPT",
                    "PRETORIA" => "TSH",
                    "DURBAN" => "ETH",
                    "PORT ELIZABETH" => "NMA",
                    "EAST RAND" => "EKU",
                    "JOHANNESBURG" => "JHB",

                    "KZNDMA22 [HIGHMOOR/KAMBERG PARK]" => "KZNDMA22",
                    "KZNDMA23 [GAINTS CASTLE GAME RESERVE]" => "KZNDMA23",
                    "KZNDMA27 [ST LUCIA PARK]" => "KZNDMA27",
                    "KZNDMA43 [MKHOMAZI WILDERNESS AREA]" => "KZNDMA43",
                    "KZDMA43 [Mkhomazi Wilderness area]" => "KZNDMA43",

                    "LIMDMA33 [KRUGER PARK]" => "LIMDMA33",
                    "MPDMA32 [KRUGER PARK]" => "MPDMA32",
                    "NCDMA45 [KALAHARI CBDC]" => "NCDMA45",

                    "ECDMA10 [ABERDEEN PLAIN]" => "ECDMA10",
                    "ECDMA13 [MOUNT ZEBRA NP]" => "ECDMA13",
                    "ECDMA14 [OVISTON NATURE RESERVE]" => "ECDMA14",
                    "ECDMA44 [O CONNERS CAMP]" => "ECDMA44",
                    "FSDMA19 [GOLDEN GATE HIGHLANDS NP]" => "FSDMA19",
                    "KZDMA22 [HIGHMOOR/KAMBERG PARK]" => "KZDMA22",
                    "KZDMA23 [GAINTS CASTLE GAME RESERVE]" => "KZDMA23",
                    "KZDMA27 [ST LUCIA PARK]" => "KZDMA27",
                    "KZDMA43 [MKHOMAZI WILDERNESS AREA]" => "KZDMA43",
                    "CBDMA4 [KRUGER PARK]" => "CBDMA4",
                    "CBDMA3 [SCHUINSDRAAI NATURE RESERVE]" => "CBDMA3",
                    "GTDMA41 [STERKFONTEIN]" => "GTDMA41",
                    "MPDMA31 [MDALA NATURE RESERVE]" => "MPDMA31",
                    "MPDMA32 [DMA LOWVELD]" => "MPDMA32",
                    "NWDMA37 [PILANSBERG NATIONAL PARK]" => "NWDMA37",
                    "NCDMA06 [NAMAQUALAND]" => "NCDMA06",
                    "NCDMA07 [BO KAROO]" => "NCDMA07",
                    "NCDMA08 [BENEDE]" => "NCDMA08",
                    "NCDMA09 [DIAMONDFIELDS]" => "NCDMA09",
                    "NCDMACB1 [KALAHARI CBDC]" => "NCDMACB1",
                    "NPDMA33 [KRUGER PARK]" => "NPDMA33",
                    "WCDMA01 [WEST COAST DC]" => "WCDMA01",
                    "WCDMA02 [BREDE RIVER DC]" => "WCDMA02",
                    "WCDMA03 [OVERBERG DC]" => "WCDMA03",
                    "WCDMA04 [SOUTH CAPE DC]" => "WCDMA04",
                    "WCDMA05 [CENTRAL KAROO DC]" => "WCDMA05",

                    "KZ5A1" => "KZN5A1",
                    "KZ5A2" => "KZN5A2",
                    "KZ5A3" => "KZN5A3",
                    "KZ5A4" => "KZN5A4",
                    "KZ5A5" => "KZN5A5",
                    "KZ5A6" => "KZN5A6",

                    /*[94493]*/
                    "KZ211" => "KZN211",
                    /*[95051]*/
                    "KZ212" => "KZN212",
                    /*[95427]*/
                    "KZ213" => "KZN213",
                    /*[96494]*/
                    "KZ214" => "KZN214",
                    /*[97076]*/
                    "KZ215" => "KZN215",
                    /*[97389]*/
                    "KZ216" => "KZN216",
                    /*[98568]*/
                    "KZ221" => "KZN221",
                    /*[99149]*/
                    "KZ222" => "KZN222",
                    /*[99418]*/
                    "KZ223" => "KZN223",
                    /*[99603]*/
                    "KZ224" => "KZN224",
                    /*[99819]*/
                    "KZ225" => "KZN225",
                    /*[101899]*/
                    "KZ226" => "KZN226",
                    /*[102185]*/
                    "KZ227" => "KZN227",
                    /*[102444]*/
                    "KZ232" => "KZN232",
                    /*[103357]*/
                    "KZ233" => "KZN233",
                    /*[103877]*/
                    "KZ234" => "KZN234",
                    /*[104127]*/
                    "KZ235" => "KZN235",
                    /*[105063]*/
                    "KZ236" => "KZN236",
                    /*[105521]*/
                    "KZ241" => "KZN241",
                    /*[105679]*/
                    "KZ242" => "KZN242",
                    /*[106234]*/
                    "KZ244" => "KZN244",
                    /*[107240]*/
                    "KZ245" => "KZN245",
                    /*[107667]*/
                    "KZ252" => "KZN252",
                    /*[108675]*/
                    "KZ253" => "KZN253",
                    /*[108924]*/
                    "KZ254" => "KZN254",
                    /*[109366]*/
                    "KZ261" => "KZN261",
                    /*[109690]*/
                    "KZ262" => "KZN262",
                    /*[110148]*/
                    "KZ263" => "KZN263",
                    /*[110778]*/
                    "KZ265" => "KZN265",
                    /*[111404]*/
                    "KZ266" => "KZN266",
                    /*[112225]*/
                    "KZ271" => "KZN271",
                    /*[112609]*/
                    "KZ272" => "KZN272",
                    /*[113070]*/
                    "KZ273" => "KZN273",
                    /*[113154]*/
                    "KZ274" => "KZN274",
                    /*[113591]*/
                    "KZ275" => "KZN275",
                    /*[113709]*/
                    "KZ281" => "KZN281",
                    /*[113977]*/
                    "KZ282" => "KZN282",
                    /*[114841]*/
                    "KZ283" => "KZN283",
                    /*[115072]*/
                    "KZ284" => "KZN284",
                    /*[116115]*/
                    "KZ285" => "KZN285",
                    /*[116307]*/
                    "KZ286" => "KZN286",
                    /*[116741]*/
                    "KZ291" => "KZN291",
                    /*[117166]*/
                    "KZ292" => "KZN292",
                    /*[117696]*/
                    "KZ293" => "KZN293",
                    /*[118582]*/
                    "KZ294" => "KZN294",
                    /*[125427]*/
                    "NP331" => "LIM331",
                    /*[126402]*/
                    "NP332" => "LIM332",
                    /*[127378]*/
                    "NP333" => "LIM333",
                    /*[129074]*/
                    "NP334" => "LIM334",
                    /*[129405]*/
                    "NP341" => "LIM341",
                    /*[129615]*/
                    "NP342" => "LIM342",
                    /*[130041]*/
                    "NP343" => "LIM343",
                    /*[133808]*/
                    "NP344" => "LIM344",
                    /*[137426]*/
                    "NP351" => "LIM351",
                    /*[138194]*/
                    "NP352" => "LIM352",
                    /*[139174]*/
                    "NP353" => "LIM353",
                    /*[139821]*/
                    "NP354" => "LIM354",
                    /*[142811]*/
                    "NP355" => "LIM355",
                    /*[144438]*/
                    "NP361" => "LIM361",
                    /*[144708]*/
                    "NP362" => "LIM362",
                    /*[145152]*/
                    "NP364" => "LIM364",
                    /*[145279]*/
                    "NP365" => "LIM365",
                    /*[145529]*/
                    "NP366" => "LIM366",
                    /*[145694]*/
                    "NP367" => "LIM367",

                    _ => _text

                };

                if (_text.StartsWith("NP", StringComparison.OrdinalIgnoreCase)) _text = _text.Replace("NP", "LIM", StringComparison.OrdinalIgnoreCase);

                return _text.ToUpper();
            }
            public static string? RowToMunicipalityName(string _text)
            {
                if (_text.Contains("out of country", StringComparison.OrdinalIgnoreCase))
                    return "Out of Country";

                if (Clean(_text) is not string cleaned)
                    return null;

                _text = cleaned.ToUpper() switch
                {
                    "DURBAN METRO" => "Ethekwini/Durban Metropolitan Area",
                    "KENTON-ON-SEA TLC" => "Kenton-on-Sea TLC",
                    "GRAAFF-REINET RURAL TRC" => "Graaff-Reinet Rural TRC",
                    "NIEU-BETHESDA TLC" => "Nieu-Bethesda TLC",
                    "RIEBEECK-EAST TLC" => "Riebeeck-East TLC",
                    "ORANJE-WEST RLC" => "Oranje-West RLC",
                    "VEREENIGING-KOPANONG  MLC" => "Vereeniging-Kopanong MLC",
                    "ALLDAYS/LT-BUYSDORP RLC" => "Alldays / LT-Buysdorp RLC",
                    "GREATER NEBO-NORTH RLC" => "Greater Nebo-North RLC",
                    "KOEDOESRAND-REBONE RLC" => "Koedoesrand-Rebone RLC",
                    "LEVHUVHU-SHINWEDZI RLC" => "Levhuvhu-Shinwedzi RLC",
                    "MARABA-MASHASHANE / MAJA RLC" => "Maraba-Mashashane / Maja RLC",
                    "NGWARITSI/ MAKHUDU - THAMAGE RLC" => "Ngwaritsi / Makhudu-Thamage RLC",
                    "NOKO-TLOU / FETAKGOMO" => "Noko-Tlou / Fetakgomo",
                    "WARMBAD - PIENAARSRIVIER RLC" => "Warmbad-Pienaarsrivier RLC",
                    "BARKLY - WEST TLC" => "Barkly-West TLC",
                    "VICTORIA - WEST TLC" => "Victoria-West TLC",
                    "BO - LANGKLOOF TRC" => "Bo-Langkloof TRC",
                    "DANIËLSKUIL TLC" => "DANIELSKUIL TLC",

                    _ => _text
                };

                // Format "WCDMA02 [Brede River DC]"
                if (_text.Contains('-') is false)
                {
                    if (_text.Any(_ => _ == '[' || _ == ']'))
                        return Titlelise(_text.Split('[', 2)[1].Trim(' ', ']'));

                    return _text;
                }

                string[] textSplit = _text.Split('-', 2);

                string[] name = (Clean(textSplit[1]) ?? textSplit[1])
                    .Replace("]", "")
                    .Split('[', 2);

                if (name.Length != 2)
                    return name[0];

                name[0] = Titlelise(Clean(name[0]) ?? name[0]);
                name[1] = Titlelise(Clean(name[1]) ?? name[1]);

                if (name[0] == name[1])
                    return name[0];

                if (string.Equals(name[0], name[1], StringComparison.OrdinalIgnoreCase)) return Titlelise(name[0]);
                if (string.Equals(name[0], Clean(textSplit[0]), StringComparison.OrdinalIgnoreCase)) return Titlelise(name[1]);

                return string.Format("{0}, {1}", Titlelise(name[0]), Titlelise(name[1]));
            }
            public static string? RowToPartyName(string row)
            {
                row = string.Join("/", row
                    .Replace("-/", " ")
                    .Replace('\\', '/')
                    .Split('/')
                    .Select(_ => Clean(_))
                    .OfType<string>());

                row = row.ToUpper() switch
                {
                    "ACTIONSA" => "Action SA",
                    "AFRICAN NATIONAL CONGRESS" => "African National Congress",
                    "AFRICAN CHRISTIAN DEMOCRATIC PARTY" => "African Christian Democratic Party",
                    "AFRICAN CHRISTIAN ALLIANCE-AFRIKANER CHRISTEN ALLIANSIE" => "African Christian Alliance / Afrikaner Christen Alliansie",
                    "CAPE PARTY/ KAAPSE PARTY" or
                    "CAPE INDEPENDENCE PARTY/KAAPSE ONAFHANKLIKHEIDS PARTY" => "Cape Independence Party / Kaapse Onafhanklikheids Party",
                    "CIVIC ALLIANSIE/ALLIANCE" => "Civic Alliansie",
                    "CHRISTIAN UNITED MOVEMENT S.A (THE RIGHT CHOICE)" => "Christian United Movement SA (The Right Choice)",
                    "CONGRESS  OF THE PEOPLE" => "Congress Of The People",
                    "DEMOCRATIC ALLIANCE/DEMOKRATIESE ALLIANSIE" => "Democratic Alliance",
                    "DR JS MOROKA" => "Dr J S Moroka",
                    "ECOPEACE" => "Ecopeace Party",
                    "FRONT NASIONAAL" or
                    "FRONT NASIONAAL/FRONT NATIONAL" => "Front Nasionaal / Front National",
                    "DAGGA PARTY" or
                    "IQELA LENTSANGO - DAGGA PARTY" => "IQELA LENTSANGO / DAGGA PARTY",
                    "IZWI LETHU PARTY" => "Izwe Lethu Party",
                    "LEPELLE-NKUMPI" => "Lepele-Nkumpi",
                    "MAPSIXTEEN CIVIC MOVEMENT" => "Map 16 Civic Movement",
                    "NASIONALE AKSIE" => "National Alliance / Nasionale Aksie",
                    "NEW NATIONAL PARTY" or
                    "NUWE NASIONALE PARTY" or
                    "NUWE NASIONALE PARTY/NEW NATIONAL PARTY" => "New National Party / Nuwe Nasionale Party",
                    "NASIONAAL DEMOKRATIESE PARTY/NATIONAL DEMOCRATIC PARTY" => "Nasionaal Demokratiese Party / National Democratic Party",
                    "NATIONAL PEOPLES AMBASSADORS" => "National People's Ambassadors",
                    "PAN AFRICAN SOCIALIST MOVEMENT OF AZANIA" => "Pan Africanist Congress Of Azania",
                    "RISE UP AFRICA/TSOGA AFRICA" => "Rise Up Africa / Tsoga Africa",
                    "THEMBISA CONCERNED RESIDENTS' ASSOCIATION" => "Thembisa Concerned Residents Association",
                    "SOUTH AFRICAN MAINTANANCE AND ESTATE BENEFICIARIES ASSOCIATI" or
                    "SOUTH AFRICAN MAINTENANCE AND ESTATE BENEFICIARIES ASSOCIATI" => "South African Maintenance and Estate Beneficiaries Association",
                    "SOUTH AFRICAN CONCERNED RESIDENTS ORGANISATION 4 SERVICE DEL" => "South African Concerned Residents Organisation 4 Service Delivery",
                    "SUNRISE PARK PROTEA CITY AND GREENSIDE RESIDENTS' ASSOCIATI" => "Sunrise Park Protea City and Greenside Residents' Association",
                    "UNITED MAJORITY  FRONT" => "United Majority Front",
                    "UMHLABA UHLAGENE PEOPLES UNITED NATIONS" => "Umhlaba Uhlangene People's United Nations",
                    "UMEMPLOYED PEOPLE'S ASSOCIATION" => "Unemployed People's Association",
                    "VOTER'S INDEPENDENT PARTY - SA" => "Voter's Independent Party SA",
                    "VISION- VISIE 2000+" => "Vision/Visie 2000+",
                    "WORKING-TOGETHER POLITICAL PARTY" => "Working Together Political Party",

                    _ => row
                };

                row = Titlelise(row);

                return row
                    .Replace("Kz221", "KZ221")
                    .Replace(" La ", " la ")
                    .Replace("Rsa", "RSA")
                    .Replace("Christian United Movement Sa (the Right Choice)", "Christian United Movement SA (The Right Choice)")
                    .Replace("zation", "sation", StringComparison.OrdinalIgnoreCase)
                    .Replace("D'almedia ", "D'Almedia ", StringComparison.OrdinalIgnoreCase)
                    .Replace("Inrernational", "International", StringComparison.OrdinalIgnoreCase)
                    .Replace("Peoples", "People's", StringComparison.OrdinalIgnoreCase)
                    .Replace("Peoples'", "People's", StringComparison.OrdinalIgnoreCase)
                    .Replace("Residents'", "Residents", StringComparison.OrdinalIgnoreCase)
                    .Replace("Sol- Plaatjie", "Sol-Plaatjie", StringComparison.OrdinalIgnoreCase)
                    .Replace("Workers'", "Workers", StringComparison.OrdinalIgnoreCase)
                    .Replace("Worker's", "Workers", StringComparison.OrdinalIgnoreCase); 
            }
            public static int? RowToProvincePk(string row)
            {
                return Clean(row)?.ToLower() switch
                {
                    "eastern cape" => 1,
                    "free state" => 2,
                    "gauteng" => 3,
                    "kwa-zulu natal" or
                    "kwazulu natal" or
                    "kwazulu-natal" => 4,
                    "limpopo" => 5,
                    "mpumalanga" => 6,
                    "north west" => 7,
                    "northern cape" => 8,
                    "western cape" => 9,

                    _ => new int?()
                };
            }
            public static string? RowToString(string row)
            {
                return Clean(row);
            }
            public static string? RowToWard(string row)
            {
                row = row.Replace("Ward", string.Empty);
                
                return RowToString(row);
            }
        }
    }
}