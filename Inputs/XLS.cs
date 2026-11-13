using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Database.IEC.Inputs
{
	public static partial class XLS
	{
        public abstract class Seats(DataSet dataSet)
        {
            public string? MunicipalityGeo { get; set; }
            public string? MunicipalityName { get; set; }
        }
        public abstract class Seats<TRow>(DataSet dataSet) : Seats(dataSet) where TRow : Row
        {
            public List<TRow>? Rows { get; set; }
            public TRow? RowHeader { get; set; }
            public TRow? RowTotal { get; set; }
        }
        public abstract class SeatsLGE1<TRow>(DataSet dataSet) : Seats<TRow>(dataSet) where TRow : RowLGE1 { }
        public abstract class SeatsLGE2<TRow>(DataSet dataSet) : Seats<TRow>(dataSet) where TRow : RowLGE2 { }
        public abstract class SeatsLGE3<TRow>(DataSet dataSet) : Seats<TRow>(dataSet) where TRow : RowLGE3 { }

        public class Row 
        {
            public string? PartyName { get; set; }

            public virtual bool IsRow() { return false; }
            public virtual bool SetDataRow(DataRow datarow) { return false; }
        }
        public abstract class RowLGE1 : Row
        {
            public const int IndexPartyName = 2;
            public const int IndexValidVotes = 8;
            public const int IndexPerentageVotes = 9;
            public const int IndexTotalSeats = 14;
            public const int IndexWardSeats = 17;
            public const int IndexPRListSeats = 19;
            public const int IndexPercentageSeatsWon = 22;

            public const string NamePartyName = "Party Name";
            public const string NameValidVotes = "Valid Votes";
            public const string NamePerentageVotes = "% Votes";
            public const string NameTotalSeats = "Total Seats";
            public const string NameWardSeats = "Ward Seats";
            public const string NamePRListSeats = "PR List Seats";
            public const string NamePercentageSeatsWon = "% Seats Won";

            public double? PercentageSeatsWon { get; set; }
            public double? PerentageVotes { get; set; }
            public double? PRListSeats { get; set; }
            public double? TotalSeats { get; set; }
            public double? ValidVotes { get; set; }
            public double? WardSeats { get; set; }

            public override bool IsRow()
            {
                return
                    PercentageSeatsWon.HasValue &&
                    PerentageVotes.HasValue &&
                    PRListSeats.HasValue &&
                    TotalSeats.HasValue &&
                    ValidVotes.HasValue &&
                    WardSeats.HasValue;
            }
            public override bool SetDataRow(DataRow datarow)
            {
                base.SetDataRow(datarow);

                PartyName = IndexPartyName < datarow.ItemArray.Length && datarow.ItemArray[IndexPartyName] is string partyname ? CSV.Utils.RowToPartyName(partyname) : null;
                PercentageSeatsWon = IndexPercentageSeatsWon < datarow.ItemArray.Length ? datarow.ItemArray[IndexPercentageSeatsWon] as double? : null;
                PerentageVotes = IndexPerentageVotes < datarow.ItemArray.Length ? datarow.ItemArray[IndexPerentageVotes] as double? : null;
                PRListSeats = IndexPRListSeats < datarow.ItemArray.Length ? datarow.ItemArray[IndexPRListSeats] as double? : null;
                TotalSeats = IndexTotalSeats < datarow.ItemArray.Length ? datarow.ItemArray[IndexTotalSeats] as double? : null;
                ValidVotes = IndexValidVotes < datarow.ItemArray.Length ? datarow.ItemArray[IndexValidVotes] as double? : null;
                WardSeats = IndexWardSeats < datarow.ItemArray.Length ? datarow.ItemArray[IndexWardSeats] as double? : null;

                return IsRow();
            }

            public static bool IsRowHeader(DataRow datarow)
            {
                return
                    datarow.ItemArray.Contains(NamePartyName) &&
                    datarow.ItemArray.Contains(NameValidVotes) &&
                    datarow.ItemArray.Contains(NamePerentageVotes) &&
                    datarow.ItemArray.Contains(NameTotalSeats) &&
                    datarow.ItemArray.Contains(NameWardSeats) &&
                    datarow.ItemArray.Contains(NamePRListSeats) &&
                    datarow.ItemArray.Contains(NamePercentageSeatsWon);
            }
            public static bool IsRowTotal(DataRow datarow)
            {
                return datarow.ItemArray[IndexPartyName]?.Equals("Total") ?? false;
            }
        }
        public abstract class RowLGE2 : Row
        {
            public const int IndexPartyName = 0;
            public const int IndexTotalValidVotes = 5;
            public const int IndexTotalValidVotesQuota = 7;
            public const int IndexRound1Allocation = 10;
            public const int IndexRemainder = 14;
            public const int IndexRankingOfRemainder = 17;
            public const int IndexRound2Allocation = 19;
            public const int IndexTotalPartySeats = 21;

            public const string NamePartyName = "Party Name";
            public const string NameTotalValidVotes = "Total Valid Votes";
            public const string NameTotalValidVotesQuota = "Total Valid Votes / Quota";
            public const string NameRound1Allocation = "Round 1 \nAllocation";
            public const string NameRemainder = "Remainder";
            public const string NameRankingOfRemainder = "Ranking of Remainder";
            public const string NameRound2Allocation = "Round 2 \nAllocation";
            public const string NameTotalPartySeats = "Total Party Seats";

            public double? TotalValidVotes { get; set; }
            public double? TotalValidVotesQuota { get; set; }
            public int? Round1Allocation { get; set; }
            public double? Remainder { get; set; }
            public double? RankingOfRemainder { get; set; }
            public int? Round2Allocation { get; set; }
            public double? TotalPartySeats { get; set; }

            public override bool IsRow()
            {
                return
                    TotalValidVotes.HasValue &&
                    TotalPartySeats.HasValue;
            }
            public override bool SetDataRow(DataRow datarow)
            {
                base.SetDataRow(datarow);

                PartyName = IndexPartyName < datarow.ItemArray.Length && datarow.ItemArray[IndexPartyName] is string partyname ? CSV.Utils.RowToPartyName(partyname) : null;
                TotalValidVotes = IndexTotalValidVotes < datarow.ItemArray.Length ? datarow.ItemArray[IndexTotalValidVotes] as double? : null;
                TotalValidVotesQuota = IndexTotalValidVotesQuota < datarow.ItemArray.Length ? datarow.ItemArray[IndexTotalValidVotesQuota] as double? : null;
                Round1Allocation = IndexRound1Allocation < datarow.ItemArray.Length ? int.TryParse(datarow.ItemArray[IndexRound1Allocation] as string, out int _round1allocation) ? _round1allocation : null : null;
                Remainder = IndexRemainder < datarow.ItemArray.Length ? datarow.ItemArray[IndexRemainder] as double? : null;
                RankingOfRemainder = IndexRankingOfRemainder < datarow.ItemArray.Length ? datarow.ItemArray[IndexRankingOfRemainder] as double? : null;
                Round2Allocation = IndexRound2Allocation < datarow.ItemArray.Length ? int.TryParse(datarow.ItemArray[IndexRound2Allocation] as string, out int _round2allocation) ? _round2allocation : null : null;
                TotalPartySeats = IndexTotalPartySeats < datarow.ItemArray.Length ? datarow.ItemArray[IndexTotalPartySeats] as double? : null;

                return IsRow();
            }

            public static bool IsRowHeader(DataRow datarow)
            {
                return
                    datarow.ItemArray.Contains(NamePartyName) &&
                    datarow.ItemArray.Contains(NameTotalValidVotes) &&
                    datarow.ItemArray.Contains(NameTotalValidVotesQuota) &&
                    datarow.ItemArray.Contains(NameRound1Allocation) &&
                    datarow.ItemArray.Contains(NameRemainder) &&
                    datarow.ItemArray.Contains(NameRankingOfRemainder) &&
                    datarow.ItemArray.Contains(NameRound2Allocation) &&
                    datarow.ItemArray.Contains(NameTotalPartySeats);
            }
            public static bool IsRowTotal(DataRow datarow)
            {
                return datarow.ItemArray[IndexPartyName]?.Equals("Total") ?? false;
            }
        }
        public abstract class RowLGE3 : Row
        {
            public const int IndexPartyName = 0;
            public const int IndexTotalValidVotes = 3;
            public const int IndexTotalValidVotesQuota = 5;
            public const int IndexRound1Allocation = 6;
            public const int IndexRemainder = 10;
            public const int IndexRankingOfRemainder = 14;
            public const int IndexRound2Allocation = 16;
            public const int IndexTotalPartySeats = 17;

            public const string NamePartyName = "Party Name";
            public const string NameTotalValidVotes = "Total Valid Votes";
            public const string NameTotalValidVotesQuota = "Total Valid Votes / Quota";
            public const string NameRound1Allocation = "Round 1 \nAllocation";
            public const string NameRemainder = "Remainder";
            public const string NameRankingOfRemainder = "Ranking of Remainder";
            public const string NameRound2Allocation = "Round 2 \nAllocation";
            public const string NameTotalPartySeats = "Total Party Seats";

            public double? TotalValidVotes { get; set; }
            public double? TotalValidVotesQuota { get; set; }
            public int? Round1Allocation { get; set; }
            public double? Remainder { get; set; }
            public double? RankingOfRemainder { get; set; }
            public int? Round2Allocation { get; set; }
            public double? TotalPartySeats { get; set; }

            public override bool IsRow()
            {
                return
                    TotalValidVotes.HasValue &&
                    TotalPartySeats.HasValue;
            }
            public override bool SetDataRow(DataRow datarow)
            {
                base.SetDataRow(datarow);

                PartyName = IndexPartyName < datarow.ItemArray.Length && datarow.ItemArray[IndexPartyName] is string partyname ? CSV.Utils.RowToPartyName(partyname) : null;
                TotalValidVotes = IndexTotalValidVotes < datarow.ItemArray.Length ? datarow.ItemArray[IndexTotalValidVotes] as double? : null;
                TotalValidVotesQuota = IndexTotalValidVotesQuota < datarow.ItemArray.Length ? datarow.ItemArray[IndexTotalValidVotesQuota] as double? : null;
                Round1Allocation = IndexRound1Allocation < datarow.ItemArray.Length ? int.TryParse(datarow.ItemArray[IndexRound1Allocation] as string, out int _round1allocation) ? _round1allocation : null : null;
                Remainder = IndexRemainder < datarow.ItemArray.Length ? datarow.ItemArray[IndexRemainder] as double? : null;
                RankingOfRemainder = IndexRankingOfRemainder < datarow.ItemArray.Length ? datarow.ItemArray[IndexRankingOfRemainder] as double? : null;
                Round2Allocation = IndexRound2Allocation < datarow.ItemArray.Length ? int.TryParse(datarow.ItemArray[IndexRound2Allocation] as string, out int _round2allocation) ? _round2allocation : null : null;
                TotalPartySeats = IndexTotalPartySeats < datarow.ItemArray.Length ? datarow.ItemArray[IndexTotalPartySeats] as double? : null;

                return IsRow();
            }

            public static bool IsRowHeader(DataRow datarow)
            {
                return
                    datarow.ItemArray.Contains(NamePartyName) &&
                    datarow.ItemArray.Contains(NameTotalValidVotes) &&
                    datarow.ItemArray.Contains(NameTotalValidVotesQuota) &&
                    datarow.ItemArray.Contains(NameRound1Allocation) &&
                    datarow.ItemArray.Contains(NameRemainder) &&
                    datarow.ItemArray.Contains(NameRankingOfRemainder) &&
                    datarow.ItemArray.Contains(NameRound2Allocation) &&
                    datarow.ItemArray.Contains(NameTotalPartySeats);
            }
            public static bool IsRowTotal(DataRow datarow)
            {
                return datarow.ItemArray[IndexPartyName]?.Equals("Total") ?? false;
            }
        }
    }
}