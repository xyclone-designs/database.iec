using System.Data;
using System.Linq;

namespace Database.IEC.Inputs.XLSs
{
	public abstract class XLSSeatsLGE3<TRow> : XLSSeats<TRow> where TRow : XLSSeatsLGE3Row
	{
		public XLSSeatsLGE3(DataSet dataset) : base(dataset) { }
	}
	public abstract class XLSSeatsLGE3Row : XLSSeatsRow
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

			PartyName = IndexPartyName < datarow.ItemArray.Length && datarow.ItemArray[IndexPartyName] is string partyname ? CSVs.CSVRow.Utils.RowToPartyName(partyname) : null;
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