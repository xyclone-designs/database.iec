using System.Data;

namespace Database.IEC.Inputs.XLSs
{
	public class LGE2016 : XLSSeatsLGE3<LGE2016.Row>
	{
		public LGE2016(DataSet dataset) : base(dataset)
		{
			for (int index = 0; index < dataset.Tables[0].Rows.Count && dataset.Tables[0].Rows[index] is DataRow datarow; index++)
				switch (true)
				{
					case true when Row.IsRowHeader(datarow):
						(RowHeader = new Row()).SetDataRow(datarow);
						break;

					case true when Row.IsRowTotal(datarow):
						(RowTotal = new Row()).SetDataRow(datarow);
						break;

					default:
						Row row = new();
						if (row.SetDataRow(datarow))
							(Rows ??= []).Add(row);
						break;
				}
		}

		public class Row : XLSSeatsLGE3Row { }
	}
}