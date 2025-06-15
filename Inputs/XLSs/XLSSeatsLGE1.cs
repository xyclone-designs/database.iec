using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Database.IEC.Inputs.XLSs
{
    public abstract class XLSSeatsLGE1<TRow> : XLSSeats<TRow> where TRow : XLSSeatsLGE1Row
	{
		public XLSSeatsLGE1(DataSet dataset) : base(dataset) { }
	}
	public abstract class XLSSeatsLGE1Row : XLSSeatsRow
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

			PartyName = IndexPartyName < datarow.ItemArray.Length && datarow.ItemArray[IndexPartyName] is string partyname ? CSVs.CSVRow.Utils.RowToPartyName(partyname) : null;
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
}