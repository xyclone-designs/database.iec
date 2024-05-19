using System.Data;

namespace DataProcessor.XLSs
{
	public class LGE2011 : XLSSeatsLGE2<LGE2011.Row>
	{
		public LGE2011(DataSet dataset) : base(dataset)
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

		public class Row : XLSSeatsLGE2Row { }
	}
}