using System.Data;

namespace DataProcessor.XLSs
{
	public class LGE2006 : XLSSeatsLGE1<LGE2006.Row>
	{
		public LGE2006(DataSet dataset) : base(dataset)
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

		public class Row : XLSSeatsLGE1Row { }
	}
}