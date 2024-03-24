using System.Collections.Generic;
using System.Linq;

namespace DataProcessor.CSVs
{
    public class PE1994 : CSVRowNPE1
    {
        public PE1994(string line) : base(line) { }

        public static IEnumerable<PE1994> Rows()
        {
            // Taken from PDF File in Data

            return new object[][]
            {
                [ "EASTERN CAPE", "Pan Africanist Congress Of Azania", 59_475 ],
                [ "EASTERN CAPE", "Vryheidsfront/Freedom Front", 23_167 ],
                [ "EASTERN CAPE", "African Christian Democratic Party", 14_908 ],
                [ "EASTERN CAPE", "African Democratic Movement", 4_815 ],
                [ "EASTERN CAPE", "African National Congress", 2_453_790 ],
                [ "EASTERN CAPE", "Democratic Party", 59_644 ],
                [ "EASTERN CAPE", "Merit Party", 2_028 ],
                [ "EASTERN CAPE", "National Party", 286_029 ],
                [ "EASTERN CAPE", "Inkatha Freedom Party", 5_050 ],

                [ "MPUMALANGA", "Pan Africanist Congress Of Azania", 21_679 ],
                [ "MPUMALANGA", "Regte Party/Right Party", 921 ],
                [ "MPUMALANGA", "Vryheidsfront/Freedom Front", 75_120 ],
                [ "MPUMALANGA", "African Christian Democratic Party", 6_339 ],
                [ "MPUMALANGA", "African Democratic Movement", 5_062 ],
                [ "MPUMALANGA", "African National Congress", 1_070_052 ],
                [ "MPUMALANGA", "Democratic Party", 7_437 ],
                [ "MPUMALANGA", "National Party", 119_311 ],
                [ "MPUMALANGA", "Inkatha Freedom Party", 20_147 ],

                [ "KWAZULU-NATAL", "Pan Africanist Congress Of Azania", 26_601 ],
                [ "KWAZULU-NATAL", "Vryheidsfront/Freedom Front", 18_625 ],
                [ "KWAZULU-NATAL", "Workers International to Rebuild the Fourth International (SA)", 4_626 ],
                [ "KWAZULU-NATAL", "African Christian Democratic Party", 24_690 ],
                [ "KWAZULU-NATAL", "African Democratic Movement", 8_092 ],
                [ "KWAZULU-NATAL", "Africa Muslim Party", 17_931 ],
                [ "KWAZULU-NATAL", "African National Congress", 1_181_118 ],
                [ "KWAZULU-NATAL", "Democratic Party", 78_910 ],
                [ "KWAZULU-NATAL", "Minority Front", 48_951 ],
                [ "KWAZULU-NATAL", "National Party", 410_710 ],
                [ "KWAZULU-NATAL", "Inkatha Freedom Party", 1_844_070 ],

                [ "NORTHERN CAPE", "Pan Africanist Congress Of Azania", 3_765 ],
                [ "NORTHERN CAPE", "Vryheidsfront/Freedom Front", 24_117 ],
                [ "NORTHERN CAPE", "African Christian Democratic Party", 1_610 ],
                [ "NORTHERN CAPE", "African Democratic Movement", 734 ],
                [ "NORTHERN CAPE", "African National Congress", 200_839 ],
                [ "NORTHERN CAPE", "Democratic Party", 7_567 ],
                [ "NORTHERN CAPE", "National Party", 163_452 ],
                [ "NORTHERN CAPE", "Inkatha Freedom Party", 1_688 ],

                [ "LIMPOPO", "Pan Africanist Congress Of Azania", 24_360 ],
                [ "LIMPOPO", "United People's Front", 10_123 ],
                [ "LIMPOPO", "Vryheidsfront/Freedom Front", 41_193 ],
                [ "LIMPOPO", "Ximoko Progressive Party", 4_963 ],
                [ "LIMPOPO", "African Christian Democratic Party", 7_363 ],
                [ "LIMPOPO", "African Democratic Movement", 3_662 ],
                [ "LIMPOPO", "African National Congress", 1_759_597 ],
                [ "LIMPOPO", "Democratic Party", 4_021 ],
                [ "LIMPOPO", "National Party", 62_745 ],
                [ "LIMPOPO", "Inkatha Freedom Party", 2_233 ],

                [ "NORTH WEST", "Pan Africanist Congress Of Azania", 27_274 ],
                [ "NORTH WEST", "Vryheidsfront/Freedom Front", 72_821 ],
                [ "NORTH WEST", "African Christian Democratic Party", 5_570 ],
                [ "NORTH WEST", "African Democratic Movement", 3_569 ],
                [ "NORTH WEST", "African National Congress", 1_310_080 ],
                [ "NORTH WEST", "Democratic Party", 7_894 ],
                [ "NORTH WEST", "National Party", 138_986 ],
                [ "NORTH WEST", "Inkatha Freedom Party", 5_948 ],

                [ "FREE STATE", "Pan Africanist Congress Of Azania", 24_451 ],
                [ "FREE STATE", "Vryheidsfront/Freedom Front", 81_662 ],
                [ "FREE STATE", "African Christian Democratic Party", 6_072 ],
                [ "FREE STATE", "African Democratic Movement", 2_008 ],
                [ "FREE STATE", "African National Congress", 1_037_998 ],
                [ "FREE STATE", "Democratic Party", 7_664 ],
                [ "FREE STATE", "Dikwankwetla Party Of South Africa", 17_024 ],
                [ "FREE STATE", "National Party", 170_452 ],
                [ "FREE STATE", "Inkatha Freedom Party", 6_935 ],

                [ "GAUTENG", "Pan Africanist Congress Of Azania", 61_512 ],
                [ "GAUTENG", "Vryheidsfront/Freedom Front", 258_935 ],
                [ "GAUTENG", "Women's Rights Peace Party", 7_279 ],
                [ "GAUTENG", "Ximoko Progressive Party", 3_275 ],
                [ "GAUTENG", "African Christian Democratic Party", 25_542 ],
                [ "GAUTENG", "African Democratic Movement", 4_352 ],
                [ "GAUTENG", "Africa Muslim Party", 12_888 ],
                [ "GAUTENG", "African National Congress", 2_418_257 ],
                [ "GAUTENG", "Democratic Party", 223_548 ],
                [ "GAUTENG", "Dikwankwetla Party Of South Africa", 4_853 ],
                [ "GAUTENG", "Federal Party", 16_279 ],
                [ "GAUTENG", "Luso South African Party", 5_423 ],
                [ "GAUTENG", "National Party", 1_002_540 ],
                [ "GAUTENG", "Inkatha Freedom Party", 153_567 ],

                [ "WESTERN CAPE", "Pan Africanist Congress Of Azania", 22_676 ],
                [ "WESTERN CAPE", "South African Woman's Party", 2_641 ],
                [ "WESTERN CAPE", "Green Party", 2_611 ],
                [ "WESTERN CAPE", "Vryheidsfront/Freedom Front", 44_003 ],
                [ "WESTERN CAPE", "Wes-Kaap Federaliste Party", 6_337 ],
                [ "WESTERN CAPE", "Workers International to Rebuild the Fourth International (SA)", 855 ],
                [ "WESTERN CAPE", "African Christian Democratic Party", 25_731 ],
                [ "WESTERN CAPE", "African Democratic Movement", 1_939 ],
                [ "WESTERN CAPE", "Africa Muslim Party", 20_954 ],
                [ "WESTERN CAPE", "African National Congress", 705_576 ],
                [ "WESTERN CAPE", "Democratic Party", 141_970 ],
                [ "WESTERN CAPE", "Islamic Party", 16_762 ],
                [ "WESTERN CAPE", "National Party", 1_138_242 ],
                [ "WESTERN CAPE", "Inkatha Freedom Party", 7_445 ],

            }.Select(array => 
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

                return new PE1994(string.Format(
                    "1994 PROVINCIAL ELECTION,{0},_,_,{1},_,_,{2},_,_,",
                    array[0], array[1], array[2]));
            });
        }
        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsProvincial(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(1994, electoralEvent.date);
        }
    }
}