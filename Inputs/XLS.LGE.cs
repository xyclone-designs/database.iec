using System.Data;

namespace Database.IEC.Inputs
{
	public static partial class XLS
	{
        public static class LGE
        {
            public class _2000 : SeatsLGE1<_2000.Row>
            {
                public _2000(DataSet dataset) : base(dataset)
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

                public class Row : RowLGE1 { }
            }
            public class _2006 : SeatsLGE1<_2006.Row>
            {
                public _2006(DataSet dataset) : base(dataset)
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

                public class Row : RowLGE1 { }
            }
            public class _2011 : SeatsLGE2<_2011.Row>
            {
                public _2011(DataSet dataset) : base(dataset)
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

                public class Row : RowLGE2 { }
            }
            public class _2016 : SeatsLGE3<_2016.Row>
            {
                public _2016(DataSet dataset) : base(dataset)
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

                public class Row : RowLGE3 { }
            }
            public class _2021 : SeatsLGE3<_2021.Row>
            {
                public _2021(DataSet dataset) : base(dataset)
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

                public class Row : RowLGE3 { }
            }
            public class _2026 : SeatsLGE3<_2026.Row>
            {
                public _2026(DataSet dataset) : base(dataset)
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

                public class Row : RowLGE3 { }
            }
        }
    }
}