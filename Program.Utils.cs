using DataProcessor.Tables;

using ExcelDataReader;

using SQLite;

using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace DataProcessor
{
    internal partial class Program
    {
        static void UtilsCSVLog<TCSVRow>(SQLiteConnection sqliteConnection, StreamWriter log, IEnumerable<TCSVRow> rows) where TCSVRow : CSVs.CSVRow
        {
            foreach (TCSVRow row in rows)
            {
                Operations.Log<Municipality, TCSVRow>(row, log);
                Operations.Log<Party, TCSVRow>(row, log);
                Operations.Log<VotingDistrict, TCSVRow>(row, log);
                Operations.Log<Ward, TCSVRow>(row, log);
            }
        }
        static IEnumerable<TCSVRow> UtilsCSVRows<TCSVRow>(StreamWriter log, params string[] filepaths) where TCSVRow : CSVs.CSVRow
        {
            foreach (string filepath in filepaths)
            {
                using FileStream fileStream = File.OpenRead(filepath);
                using StreamReader streamReader = new(fileStream);

                log.WriteLine(filepath);
                log.WriteLine();
                streamReader.ReadLine();

                for (int linecurrent = 1; streamReader.ReadLine() is string line; linecurrent++)
                {
                    TCSVRow? row = default;
                    string[] columns = line.Split(',');

                    try
                    {
                        row = true switch
                        {
                            true when typeof(TCSVRow) == typeof(CSVs.NE1994) => new CSVs.NE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.PE1994) => new CSVs.PE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE1999) => new CSVs.NPE1999(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2000) => new CSVs.LGE2000(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE2004) => new CSVs.NPE2004(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2006) => new CSVs.LGE2006(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE2009) => new CSVs.NPE2009(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2011) => new CSVs.LGE2011(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE2014) => new CSVs.NPE2014(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2016) => new CSVs.LGE2016(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NE2019) => new CSVs.NE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.PE2019) => new CSVs.PE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2021) => new CSVs.LGE2021(line) { LineNumber = linecurrent } as TCSVRow,

                            _ => throw new Exception(),
                        };
                    }
                    catch (Exception ex) { log.WriteLine("[{0}]: Error = '{1}'", linecurrent, ex.Message); }

                    if (row is not null)
                        yield return row;
                }
            }
        }
        static IEnumerable<TCSVRow> UtilsCSVRows<TCSVRow>(StreamWriter log, params Stream[] streams) where TCSVRow : CSVs.CSVRow
        {
            foreach (Stream stream in streams)
            {
                using StreamReader streamReader = new(stream);

                log.WriteLine();
                streamReader.ReadLine();

                for (int linecurrent = 1; streamReader.ReadLine() is string line; linecurrent++)
                {
                    TCSVRow? row = default;
                    string[] columns = line.Split(',');

                    try
                    {
                        row = true switch
                        {
                            true when typeof(TCSVRow) == typeof(CSVs.NE1994) => new CSVs.NE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.PE1994) => new CSVs.PE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE1999) => new CSVs.NPE1999(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2000) => new CSVs.LGE2000(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE2004) => new CSVs.NPE2004(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2006) => new CSVs.LGE2006(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE2009) => new CSVs.NPE2009(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2011) => new CSVs.LGE2011(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NPE2014) => new CSVs.NPE2014(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2016) => new CSVs.LGE2016(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.NE2019) => new CSVs.NE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.PE2019) => new CSVs.PE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(CSVs.LGE2021) => new CSVs.LGE2021(line) { LineNumber = linecurrent } as TCSVRow,

                            _ => throw new Exception(),
                        };
                    }
                    catch (Exception ex) { log.WriteLine("[{0}]: Error = '{1}'", linecurrent, ex.Message); }

                    if (row is not null)
                        yield return row;
                }
            }
        }
        static IEnumerable<TXLSSeats> UtilsXLSSeats<TXLSSeats>(ZipArchive datazip, string prefix, Func<DataSet, TXLSSeats> create) where TXLSSeats : XLSs.XLSSeats
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

                seats.MunicipalityGeo = CSVs.CSVRow.Utils.RowToMunicipalityGeo(entry.FullName.Split('\\', '.', '_')[^2]);

				yield return seats;
			}
		}
	}
}
