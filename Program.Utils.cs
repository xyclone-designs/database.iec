using Database.IEC.Inputs.CSVs;
using Database.IEC.Inputs.XLSs;

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
        static IEnumerable<TCSVRow> UtilsCSVRows<TCSVRow>(StreamWriter log, params string[] filepaths) where TCSVRow : Inputs.CSVs.CSVRow
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
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NE1994) => new Inputs.CSVs.NE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.PE1994) => new Inputs.CSVs.PE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE1999) => new Inputs.CSVs.NPE1999(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2000) => new Inputs.CSVs.LGE2000(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE2004) => new Inputs.CSVs.NPE2004(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2006) => new Inputs.CSVs.LGE2006(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE2009) => new Inputs.CSVs.NPE2009(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2011) => new Inputs.CSVs.LGE2011(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE2014) => new Inputs.CSVs.NPE2014(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2016) => new Inputs.CSVs.LGE2016(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NE2019) => new Inputs.CSVs.NE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.PE2019) => new Inputs.CSVs.PE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2021) => new Inputs.CSVs.LGE2021(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NE2024) => new Inputs.CSVs.NE2024(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.PE2024) => new Inputs.CSVs.PE2024(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.RE2024) => new Inputs.CSVs.RE2024(line) { LineNumber = linecurrent } as TCSVRow,

                            _ => throw new Exception(),
                        };
                    }
                    catch (Exception ex) { log.WriteLine("[{0}]: Error = '{1}'", linecurrent, ex.Message); }

                    if (row is not null)
                        yield return row;
                }
            }
        }
        static IEnumerable<TCSVRow> UtilsCSVRows<TCSVRow>(StreamWriter log, params Stream[] streams) where TCSVRow : Inputs.CSVs.CSVRow
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
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NE1994) => new Inputs.CSVs.NE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.PE1994) => new Inputs.CSVs.PE1994(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE1999) => new Inputs.CSVs.NPE1999(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2000) => new Inputs.CSVs.LGE2000(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE2004) => new Inputs.CSVs.NPE2004(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2006) => new Inputs.CSVs.LGE2006(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE2009) => new Inputs.CSVs.NPE2009(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2011) => new Inputs.CSVs.LGE2011(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NPE2014) => new Inputs.CSVs.NPE2014(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2016) => new Inputs.CSVs.LGE2016(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NE2019) => new Inputs.CSVs.NE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.PE2019) => new Inputs.CSVs.PE2019(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.LGE2021) => new Inputs.CSVs.LGE2021(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.NE2024) => new Inputs.CSVs.NE2024(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.PE2024) => new Inputs.CSVs.PE2024(line) { LineNumber = linecurrent } as TCSVRow,
                            true when typeof(TCSVRow) == typeof(Inputs.CSVs.RE2024) => new Inputs.CSVs.RE2024(line) { LineNumber = linecurrent } as TCSVRow,

                            _ => throw new Exception(),
                        };
                    }
                    catch (Exception ex) { log.WriteLine("[{0}]: Error = '{1}'", linecurrent, ex.Message); }

                    if (row is not null)
                        yield return row;
                }
            }
        }
        static IEnumerable<TXLSSeats> UtilsXLSSeats<TXLSSeats>(ZipArchive datazip, string prefix, Func<DataSet, TXLSSeats> create) where TXLSSeats : Inputs.XLSs.XLSSeats
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

                seats.MunicipalityGeo = Inputs.CSVs.CSVRow.Utils.RowToMunicipalityGeo(entry.FullName.Split('\\', '.', '_')[^2]);

				yield return seats;
			}
		}
	}
}
