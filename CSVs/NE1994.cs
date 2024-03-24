using System.Collections.Generic;
using System.Linq;

namespace DataProcessor.CSVs
{
    public class NE1994 : CSVRowNPE1
    {
        public NE1994(string line) : base(line) { }

        private static Dictionary<string, int[]> PartyAndVotes = new()
        {
            { "Pan Africanist Congress of Azania", [ 56_891, 17_800, 23_098, 3_941, 20_295, 24_233, 23_310, 52_557, 21_353, 243_478 ] },
            { "Sport Organisation For Collective Contributions And Equal Rights", [ 918, 636, 2_311, 245, 666, 959, 857, 2_953, 1_030, 10_575 ] },
            { "Keep It Straight And Simple", [ 900, 415, 1_010, 293, 365, 548, 403, 1_107, 875, 5_916 ] },
            { "Vryheidsfront / Freedom Front", [ 18_656, 45_964, 17_092, 17_480, 29_000, 49_175, 50_386, 154_878, 41_924, 424_555 ] },
            { "Women's Rights Peace Party", [ 524, 311, 955, 151, 273, 568, 398, 1_850, 1_404, 6_434 ] },
            { "Workers List Party", [ 374, 309, 1_193, 167, 259, 331, 258, 554, 724, 4_169 ] },
            { "Ximoko Progressive Party", [ 574, 416, 1_501, 113, 1_354, 578, 683, 828, 273, 6_320 ] },
            { "Africa Muslim Party", [ 1_235, 906, 6_790, 320, 437, 1_386, 324, 7_413, 15_655, 34_466 ] },
            { "African Christian Democratic Party", [ 10_879, 4_474, 17_122, 1_294, 5_042, 3_901, 4_523, 20_329, 20_540, 88_104 ] },
            { "African Democratic Movement", [ 1_869, 611, 3_819, 189, 597, 701, 553, 1_062, 485, 9_886 ] },
            { "African Moderates Congress Party", [ 4_919, 2_625, 3_305, 864, 3_168, 3_244, 2_644, 5_635, 1_286, 27_690 ] },
            { "African National Congress", [ 2_411_695, 1_072_518, 1_185_669, 201_515, 1_780_177, 1_325_559, 1_059_313, 2_486_938, 714_271, 12_237_655 ] },
            { "Democratic Party", [ 35_435, 5_492, 60_499, 5_235, 3_402, 5_826, 7_365, 126_368, 88_804, 338_426 ] },
            { "Dikwankwetla Party of South Africa", [ 1_098, 834, 1_927, 415, 722, 2_088, 8_796, 2_424, 1_147, 19_451 ] },
            { "Federal Party", [ 750, 527, 3_347, 162, 310, 500, 519, 6_844, 4_704, 17_663 ] },
            { "Luso South African Party", [ 263, 269, 961, 138, 253, 252, 203, 490, 464, 3_293 ] },
            { "Minority Front", [ 981, 503, 6_410, 494, 662, 772, 490, 1_575, 1_546, 13_433 ] },
            { "National Party", [ 302_951, 134_511, 591_212, 169_661, 69_870, 160_479, 198_780, 1_160_593, 1_195_633, 3_983_690 ] },
            { "Inkatha Freedom Party", [ 6_798, 20_872, 1_822_385, 1_902, 2_938, 7_155, 8_446, 173_903, 13_895, 2_058_294 ] },
        };
        public static IEnumerable<NE1994> Rows()
        {
            // Taken from PDF File in Data

            return PartyAndVotes.SelectMany((partyvotes, index) =>
            {
                /*
                 * 0 ELECTORAL EVENT 
                 * 1 PROVINCE 
                 * 2 MUNICIPALITY 
                 * 3 VOTING DISTRICT 
                 * 4 PARTY NAME 
                 * 5 REGISTERED VOTERS 
                 * 6 % VOTER TURNOUT 
                 * 7 VALID VOTES 
                 * 8 SPOILT VOTES 
                 * 9 TOTAL VOTES CAST
                 */

                return Enumerable.Range(0, 9).Select(number =>
                {
                    string partyname = partyvotes.Key;
                    string province = number switch
                    {
                        0 => "EASTERN CAPE",
                        1 => "MPUMALANGA",
                        2 => "KWA-ZULU NATAL",
                        3 => "NORTHERN CAPE",
                        4 => "LIMPOPO",
                        5 => "NORTH WEST",
                        6 => "FREE STATE",
                        7 => "GAUTENG",
                        8 => "WESTERN CAPE",

                        _ => throw new Exception("NE1994 Rows Select province"),
                    };

                    string line = string.Format("1994 NATIONAL ELECTION,{0},_,_,{1},_,_,{2},_,_,", province, partyname, partyvotes.Value[number]);

                    return new NE1994(line);
                });
            });
        }
        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsNational(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(1994, electoralEvent.date);
        }
    }
}