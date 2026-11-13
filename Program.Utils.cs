using Database.IEC.Inputs;

using ExcelDataReader;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace Database.IEC
{
    internal partial class Program
    {
        static IEnumerable<TCSVRow> UtilsCSVRows<TCSVRow>(StreamWriter log, IEnumerable<Stream> streams) where TCSVRow : CSV.Row
        {
            return UtilsCSVRows<TCSVRow>(log, [.. streams]);
        }
        static IEnumerable<TCSVRow> UtilsCSVRows<TCSVRow>(StreamWriter log, params Stream[] streams) where TCSVRow : CSV.Row
        {
            foreach (Stream stream in streams)
            {
                using StreamReader streamReader = new(stream);

                log.WriteLine();
                streamReader.ReadLine();

                for (int linecurrent = 1; streamReader.ReadLine() is string line && string.IsNullOrWhiteSpace(line) is false; linecurrent++)
                {
                    TCSVRow? row = default;
                    string[] columns = line.Split(',');

                    try
                    {
                        row = true switch
                        {
                            true when typeof(TCSVRow) == typeof(CSV.NE._1994) => new CSV.NE._1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.PE._1994) => new CSV.PE._1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.NPE._1999) => new CSV.NPE._1999(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.LGE._2000) => new CSV.LGE._2000(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.NPE._2004) => new CSV.NPE._2004(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.LGE._2006) => new CSV.LGE._2006(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.NPE._2009) => new CSV.NPE._2009(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.LGE._2011) => new CSV.LGE._2011(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.NPE._2014) => new CSV.NPE._2014(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.LGE._2016) => new CSV.LGE._2016(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.NE._2019) => new CSV.NE._2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.PE._2019) => new CSV.PE._2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.LGE._2021) => new CSV.LGE._2021(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.NE._2024) => new CSV.NE._2024(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.PE._2024) => new CSV.PE._2024(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.RE._2024) => new CSV.RE._2024(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSV.LGE._2026) => new CSV.LGE._2026(line) { LineNumber = linecurrent } as TCSVRow,

                            _ => throw new Exception(),
                        };
                    }
                    catch (Exception ex) { log.WriteLine("[{0}]: Error = '{1}'", linecurrent, ex.Message); }

                    if (row is not null)
                        yield return row;
                }
            }
        }
        static IEnumerable<TXLSSeats> UtilsXLSSeats<TXLSSeats>(ZipArchive datazip, string prefix, Func<DataSet, TXLSSeats> create) where TXLSSeats : XLS.Seats
		{
			Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

			foreach (ZipArchiveEntry entry in datazip.Entries
				.Where(entry =>
				{
					return
						entry.FullName.StartsWith("lge-seats") &&
						entry.FullName.Contains(prefix) &&
						entry.FullName.EndsWith(".xls");
				}))
			{
				using Stream entrystream = entry.Open();
				using MemoryStream entrymemorystream = new();
				entrystream.CopyTo(entrymemorystream);
				using IExcelDataReader exceldatareader = ExcelReaderFactory.CreateReader(entrymemorystream);
				using DataSet dataset = exceldatareader.AsDataSet();

				TXLSSeats seats = create.Invoke(dataset);

                seats.MunicipalityGeo = CSV.Utils.RowToMunicipalityGeo(entry.FullName.Split('\\', '.', '_')[^2]);

				yield return seats;
			}
		}
	}
}
