using System.Collections.Generic;
using System.Data;

namespace Database.IEC.Inputs.XLSs
{
	public abstract class XLSSeats
	{
		public XLSSeats(DataSet dataset) { }

		public string? MunicipalityGeo { get; set; }
		public string? MunicipalityName { get; set; }
	}
    public abstract class XLSSeats<TRow> : XLSSeats where TRow : XLSSeatsRow
	{
        public XLSSeats(DataSet dataset) : base(dataset) { }

		public List<TRow>? Rows { get; set; }
		public TRow? RowHeader { get; set; }
		public TRow? RowTotal { get; set; }
	}
	public class XLSSeatsRow
	{
		public XLSSeatsRow() { }

		public string? PartyName { get; set; }

		public virtual bool IsRow() { return false; }
		public virtual bool SetDataRow(DataRow datarow) { return false; }
	}
}