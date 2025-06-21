using ICSharpCode.SharpZipLib.GZip;

using Newtonsoft.Json.Linq;

using SQLite;

using System;
using System.Data;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

using XycloneDesigns.Apis.IEC.Tables;
using XycloneDesigns.Apis.General.Tables;

using _TableGeneral = XycloneDesigns.Apis.General.Tables._Table;

namespace Database.IEC
{
    internal partial class Program
    {
		//static readonly string DirectoryCurrent = Directory.GetCurrentDirectory();
		static readonly string DirectoryCurrent = Directory.GetParent(Directory.GetCurrentDirectory())?.Parent?.Parent?.FullName!;

		static readonly string DirectoryOutput = Path.Combine(DirectoryCurrent, ".output");
		static readonly string DirectoryInput = Path.Combine(DirectoryCurrent, ".input");
		static readonly string DirectoryInputMunicipalities = Path.Combine(DirectoryInput, ".input");

		static void Main(string[] args)
		{
			if (Directory.Exists(DirectoryOutput)) Directory.Delete(DirectoryOutput, true); Directory.CreateDirectory(DirectoryOutput);

			string 
                dbname = "iec.db", 
                dbnameversioned = "iec.1.db", 
                dbmunicipalitiesname = "municipalities.db", 
                dbprovincesname = "provinces.db", 
                logname = "log.txt";

			string 
                dbpath = Path.Combine(DirectoryOutput, dbname), 
                dbmunicipalitiespath = Path.Combine(DirectoryOutput, dbmunicipalitiesname), 
                dbprovincespath = Path.Combine(DirectoryOutput, dbprovincesname), 
                dbpathversioned = Path.Combine(DirectoryOutput, dbnameversioned), 
                logpath = Path.Combine(DirectoryOutput, logname), 
                datapath = Path.Combine(DirectoryCurrent, ".inputs", "data.zip");

            int[] pksElectoralEvent =
            [
                01,
                02,
                03,
                04,
                05,
                06,
                07,
                08,
                09,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
            ];

			if (File.Exists(dbpath)) File.Delete(dbpath);
            if (File.Exists(dbpathversioned)) File.Delete(dbpathversioned);
            if (File.Exists(logpath)) File.Delete(logpath);

            if (Path.Combine(DirectoryInput, dbmunicipalitiesname) is string _dbmunicipalitiespath && 
                File.Exists(_dbmunicipalitiespath)) File.Copy(_dbmunicipalitiespath, dbmunicipalitiespath);
			if (Path.Combine(DirectoryInput, dbprovincesname) is string _dbprovincespath && 
                File.Exists(_dbprovincespath)) File.Copy(_dbprovincespath, dbprovincespath);

			if (File.Exists(logpath)) File.Delete(logpath);

            using FileStream datastream = File.OpenRead(datapath);
            using FileStream logfilestream = File.Open(logpath, FileMode.OpenOrCreate);
            using StreamWriter logstreamwriter = new(logfilestream);
            using ZipArchive datazip = new(datastream);

			SQLiteConnection(dbpathversioned).Close();

            JArray apifiles = [];
			List<ElectoralEvent> electoralevents = [];
			Parameters parameters = new()
            {
				Logger = logstreamwriter,

				ElectoralEvents = [],
                Parties = [],
                Provinces = [],
                Municipalities = [],
                VotingDistricts = [],
                Wards = [],

				SqliteConnection = SQLiteConnection(dbpath),
				SqliteConnectionMunicipalities = new SQLiteConnection(dbmunicipalitiespath),
				SqliteConnectionProvinces = new SQLiteConnection(dbprovincespath),
			};

			parameters.SqliteConnectionMunicipalities.CreateTable<Municipality>();
			parameters.SqliteConnectionProvinces.CreateTable<Province>();

			if (parameters.SqliteConnectionProvinces is not null)
				datazip.ReadDataProvinces(parameters.SqliteConnectionProvinces, parameters.Logger);

            if (parameters.SqliteConnection is not null)
            {
                datazip.ReadDataParties(parameters.SqliteConnection, logstreamwriter);
                datazip.ReadDataElectoralEvents(parameters.SqliteConnection, logstreamwriter);

                electoralevents = [.. parameters.SqliteConnection.Table<ElectoralEvent>()];
            }
            else
            {
				using SQLiteConnection _sqliteconnection = SQLiteConnection(dbpath);

				datazip.ReadDataParties(_sqliteconnection, logstreamwriter);
				datazip.ReadDataElectoralEvents(_sqliteconnection, logstreamwriter);

				electoralevents = [.. _sqliteconnection.Table<ElectoralEvent>()];
			}

			if (parameters.SqliteConnectionMunicipalities is not null)
				datazip.ReadDataMunicipalities(parameters.SqliteConnectionMunicipalities, parameters.Logger);

			// 1994 National
			if (pksElectoralEvent.Contains(01)) Process(new Parameters<Inputs.CSVs.NE1994>(parameters)
			{
				LoggerTag = nameof(Inputs.CSVs.NE1994),
				Rows1 = Inputs.CSVs.NE1994.Rows().ToList(),
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.NE1994.IsElectoralEvent(_)),
			});

			// 1994 Provincial
			if (pksElectoralEvent.Contains(02)) Process(new Parameters<Inputs.CSVs.PE1994>(parameters)
			{
				LoggerTag = nameof(Inputs.CSVs.PE1994),
				Rows1 = Inputs.CSVs.PE1994.Rows().ToList(),
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.PE1994.IsElectoralEvent(_)),
			});

            // 1999 National & Provincial
            if (pksElectoralEvent.Contains(03) is bool NE1999 && pksElectoralEvent.Contains(04) is bool PE1999 && (NE1999 || PE1999))
				Process(new Parameters<Inputs.CSVs.NPE1999>(parameters)
				{
					LoggerTag = nameof(Inputs.CSVs.NPE1999),

                    ElectoralEvent1 = !NE1999 ? null : electoralevents.First(_ => Inputs.CSVs.NPE1999.IsElectoralEventNE(_)),
					Rows1 = !NE1999 ? [] : UtilsCSVRows<Inputs.CSVs.NPE1999>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("1999 NPE.csv")).Open())
                        .Where(_ => _.GetBallotType() == ElectoralEvent.Types.National)
                        .ToList(),

					ElectoralEvent2 = !PE1999 ? null : electoralevents.First(_ => Inputs.CSVs.NPE1999.IsElectoralEventPE(_)),
					Rows2 = !PE1999 ? null : UtilsCSVRows<Inputs.CSVs.NPE1999>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("1999 NPE.csv")).Open())
                        .Where(_ => _.GetBallotType() == ElectoralEvent.Types.Provincial)
                        .ToList(),
				});

            // 2000 Municipal
            if (pksElectoralEvent.Contains(05)) Process(new Parameters<Inputs.CSVs.LGE2000>(parameters)
			{
				LoggerTag = "LGE2000",
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.LGE2000.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.LGE2000>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2000 LGE.csv")).Open()).ToList(),
				ExtraAction = parametersextra =>
                {
                    if (parametersextra.SqliteConnection is null || parametersextra.ElectoralEvent1 is null)
                        return;

					XLS<Inputs.XLSs.LGE2000, Inputs.XLSs.LGE2000.Row>(
						parametersextra.SqliteConnection,
						parametersextra.ElectoralEvent1, 
                        UtilsXLSSeats(datazip, "LGE2000", dataset => new Inputs.XLSs.LGE2000(dataset)).ToList());

					ElectoralEventLGEUpdate(parametersextra.SqliteConnection, parametersextra.ElectoralEvent1, parametersextra.SqliteConnection.Table<Ballot>());
				}
			});

            // 2004 National & Provincial
            if (pksElectoralEvent.Contains(06) is bool NE2004 && pksElectoralEvent.Contains(07) is bool PE2004 && (NE2004 || PE2004))
				Process(new Parameters<Inputs.CSVs.NPE2004>(parameters)
				{
					LoggerTag = nameof(Inputs.CSVs.NPE2004),

					ElectoralEvent1 = !NE2004 ? null : electoralevents.First(_ => Inputs.CSVs.NPE2004.IsElectoralEventNE(_)),
					Rows1 = !NE2004 ? [] : UtilsCSVRows<Inputs.CSVs.NPE2004>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2004 NPE.csv")).Open())
						.Where(_ => _.GetBallotType() == ElectoralEvent.Types.National)
						.ToList(),

					ElectoralEvent2 = !PE2004 ? null : electoralevents.First(_ => Inputs.CSVs.NPE2004.IsElectoralEventPE(_)),
					Rows2 = !PE2004 ? null : UtilsCSVRows<Inputs.CSVs.NPE2004>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2004 NPE.csv")).Open())
						.Where(_ => _.GetBallotType() == ElectoralEvent.Types.Provincial)
						.ToList(),
				});

            // 2006 Municipal
            if (pksElectoralEvent.Contains(08)) Process(new Parameters<Inputs.CSVs.LGE2006>(parameters)
			{
				LoggerTag = "LGE2006",
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.LGE2006.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.LGE2006>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2006 LGE.csv")).Open()).ToList(),
				ExtraAction = parametersextra =>
				{
					if (parametersextra.SqliteConnection is null || parametersextra.ElectoralEvent1 is null)
						return;

					XLS<Inputs.XLSs.LGE2006, Inputs.XLSs.LGE2006.Row>(
						parametersextra.SqliteConnection,
						parametersextra.ElectoralEvent1,
						UtilsXLSSeats(datazip, "LGE2006", dataset => new Inputs.XLSs.LGE2006(dataset)).ToList());

					ElectoralEventLGEUpdate(parametersextra.SqliteConnection, parametersextra.ElectoralEvent1, parametersextra.SqliteConnection.Table<Ballot>());
				}
			});

            // 2009 National & Provincial
            if (pksElectoralEvent.Contains(09) is bool NE2009 && pksElectoralEvent.Contains(10) is bool PE2009 && (NE2009 || PE2009))
				Process(new Parameters<Inputs.CSVs.NPE2009>(parameters)
				{
					LoggerTag = nameof(Inputs.CSVs.NPE2009),

					ElectoralEvent1 = !NE2009 ? null : electoralevents.First(_ => Inputs.CSVs.NPE2009.IsElectoralEventNE(_)),
					Rows1 = !NE2009 ? [] : UtilsCSVRows<Inputs.CSVs.NPE2009>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2009 NPE.csv")).Open())
						.Where(_ => _.GetBallotType() == ElectoralEvent.Types.National)
						.ToList(),

					ElectoralEvent2 = !PE2009 ? null : electoralevents.First(_ => Inputs.CSVs.NPE2009.IsElectoralEventPE(_)),
					Rows2 = !PE2009 ? null : UtilsCSVRows<Inputs.CSVs.NPE2009>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2009 NPE.csv")).Open())
						.Where(_ => _.GetBallotType() == ElectoralEvent.Types.Provincial)
						.ToList(),
				});

            // 2011 Municipal
            if (pksElectoralEvent.Contains(11)) Process(new Parameters<Inputs.CSVs.LGE2011>(parameters)
			{
				LoggerTag = "LGE2011",
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.LGE2011.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.LGE2011>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2011 LGE.csv")).Open()).ToList(),
				ExtraAction = parametersextra =>
				{
					if (parametersextra.SqliteConnection is null || parametersextra.ElectoralEvent1 is null)
						return;

					XLS<Inputs.XLSs.LGE2011, Inputs.XLSs.LGE2011.Row>(
						parametersextra.SqliteConnection,
						parametersextra.ElectoralEvent1,
						UtilsXLSSeats(datazip, "LGE2011", dataset => new Inputs.XLSs.LGE2011(dataset)).ToList());

					ElectoralEventLGEUpdate(parametersextra.SqliteConnection, parametersextra.ElectoralEvent1, parametersextra.SqliteConnection.Table<Ballot>());
				}
			});

            // 2014 National & Provincial
            if (pksElectoralEvent.Contains(12) is bool NE2014 && pksElectoralEvent.Contains(13) is bool PE2014 && (NE2014 || PE2014))
				Process(new Parameters<Inputs.CSVs.NPE2014>(parameters)
				{
					LoggerTag = nameof(Inputs.CSVs.NPE2014),

					ElectoralEvent1 = !NE2014 ? null : electoralevents.First(_ => Inputs.CSVs.NPE2014.IsElectoralEventNE(_)),
					Rows1 = !NE2014 ? [] : UtilsCSVRows<Inputs.CSVs.NPE2014>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2014 NPE.csv")).Open())
						.Where(_ => _.GetBallotType() == ElectoralEvent.Types.National)
						.ToList(),

					ElectoralEvent2 = !PE2014 ? null : electoralevents.First(_ => Inputs.CSVs.NPE2014.IsElectoralEventPE(_)),
					Rows2 = !PE2014 ? null : UtilsCSVRows<Inputs.CSVs.NPE2014>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2014 NPE.csv")).Open())
						.Where(_ => _.GetBallotType() == ElectoralEvent.Types.Provincial)
						.ToList(),
				});

            // 2016 Municipal
            if (pksElectoralEvent.Contains(14)) Process(new Parameters<Inputs.CSVs.LGE2016>(parameters)
			{
				LoggerTag = "LGE2016",
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.LGE2016.IsElectoralEvent(_)),
                Rows1 = UtilsCSVRows<Inputs.CSVs.LGE2016>(
					logstreamwriter,
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE EC.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE FS.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE GP.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE KZN.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE LIM.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE MP.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE NC.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE NW.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE WP.csv")).Open()).ToList(),
				ExtraAction = parametersextra =>
				{
					if (parametersextra.SqliteConnection is null || parametersextra.ElectoralEvent1 is null)
						return;

					XLS<Inputs.XLSs.LGE2016, Inputs.XLSs.LGE2016.Row>(
						parametersextra.SqliteConnection,
						parametersextra.ElectoralEvent1,
						UtilsXLSSeats(datazip, "LGE2016", dataset => new Inputs.XLSs.LGE2016(dataset)).ToList());

					ElectoralEventLGEUpdate(parametersextra.SqliteConnection, parametersextra.ElectoralEvent1, parametersextra.SqliteConnection.Table<Ballot>());
				}
			});

            // 2019 National
            if (pksElectoralEvent.Contains(15)) Process(new Parameters<Inputs.CSVs.NE2019>(parameters)
			{
				LoggerTag = nameof(Inputs.CSVs.NE2019),
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.NE2019.IsElectoralEvent(_)),
                Rows1 = UtilsCSVRows<Inputs.CSVs.NE2019>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2019 NE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList()
			});

            // 2019 Provincial
            if (pksElectoralEvent.Contains(16)) Process(new Parameters<Inputs.CSVs.PE2019>(parameters)
			{
				LoggerTag = nameof(Inputs.CSVs.PE2019),
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.PE2019.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.PE2019>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2019 PE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList(),
			});

            // 2021 Municipal
            if (pksElectoralEvent.Contains(17)) Process(new Parameters<Inputs.CSVs.LGE2021>(parameters)
			{
				LoggerTag = "LGE2021",
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.LGE2021.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.LGE2021>(
					logstreamwriter,
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE EC.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE FS.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE GP.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE KZN.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE LIM.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE MP.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE NC.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE NW.csv")).Open(),
					datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE WP.csv")).Open()).ToList(),
				ExtraAction = parametersextra =>
				{
					if (parametersextra.SqliteConnection is null || parametersextra.ElectoralEvent1 is null)
						return;

					XLS<Inputs.XLSs.LGE2021, Inputs.XLSs.LGE2021.Row>(
						parametersextra.SqliteConnection,
						parametersextra.ElectoralEvent1,
						UtilsXLSSeats(datazip, "LGE2021", dataset => new Inputs.XLSs.LGE2021(dataset)).ToList());

					ElectoralEventLGEUpdate(parametersextra.SqliteConnection, parametersextra.ElectoralEvent1, parametersextra.SqliteConnection.Table<Ballot>());
				}
			});

			// 2024 National
			if (pksElectoralEvent.Contains(18)) Process(new Parameters<Inputs.CSVs.NE2024>(parameters)
			{
				LoggerTag = nameof(Inputs.CSVs.NE2024),
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.NE2024.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.NE2024>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2024 NE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList(),
			});

			// 2024 Provincial
			if (pksElectoralEvent.Contains(19)) Process(new Parameters<Inputs.CSVs.PE2024>(parameters)
			{
				LoggerTag = nameof(Inputs.CSVs.PE2024),
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.PE2024.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.PE2024>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2024 PE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList(),
			});

			// 2024 Regional
			if (pksElectoralEvent.Contains(20)) Process(new Parameters<Inputs.CSVs.RE2024>(parameters)
			{
				LoggerTag = nameof(Inputs.CSVs.RE2024),
				ElectoralEvent1 = electoralevents.First(_ => Inputs.CSVs.RE2024.IsElectoralEvent(_)),
				Rows1 = UtilsCSVRows<Inputs.CSVs.RE2024>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2024 RE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList(),
			});

			#region Indiviuals

			parameters.Ballots?.Clear();
            parameters.Municipalities?.Clear();
            parameters.Parties?.Clear();
            parameters.Provinces?.Clear();
            parameters.VotingDistricts?.Clear();
            parameters.Wards?.Clear();

            foreach (ElectoralEvent electoralevent in parameters.ElectoralEvents)
            {
                string dbelectoraleventpath = ElectoralEventPath(DirectoryOutput, electoralevent, "db", out string dbelectoraleventfilename);
                string txtelectoraleventpath = ElectoralEventPath(DirectoryOutput, electoralevent, "txt");
                
                if (File.Exists(txtelectoraleventpath) is false) continue;

                using SQLiteConnection sqliteconnectionelectoralevent = SQLiteConnection(dbelectoraleventpath);
                using FileStream txtelectoraleventfilestream = File.OpenRead(txtelectoraleventpath);
                using StreamReader txtelectoraleventstreamreader = new(txtelectoraleventfilestream);

                while (txtelectoraleventstreamreader.ReadLine() is string line)
                {
                    string[] values = line.Split(',');
                    if (values.Length == 1) continue; else
                        switch (values[0])
                        {
                            case nameof(Parameters.Parties) 
                            when parameters.SqliteConnection?.Table<Party>() is IEnumerable<Party> parties:
                                parameters.Parties = parties 
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.Provinces) 
                            when parameters.SqliteConnectionProvinces?.Table<Province>().AsEnumerable() is IEnumerable<Province> provinces:
                                parameters.Provinces = provinces
									.Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.Municipalities) 
                            when parameters.SqliteConnectionMunicipalities?.Table<Municipality>() is IEnumerable<Municipality> municipalities:
                                parameters.Municipalities = municipalities 
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.VotingDistricts) 
                            when parameters.SqliteConnection?.Table<VotingDistrict>() is IEnumerable<VotingDistrict> votingdistricts:
                                parameters.VotingDistricts = votingdistricts 
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.Wards) 
                            when parameters.SqliteConnection?.Table<Ward>() is IEnumerable<Ward> wards:
                                parameters.Wards = wards
									.Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            default: break;
                        }
                }

                txtelectoraleventfilestream.Close();
                sqliteconnectionelectoralevent.Insert(electoralevent);
                sqliteconnectionelectoralevent.InsertAll(parameters.Parties);
                sqliteconnectionelectoralevent.InsertAll(parameters.VotingDistricts);
                sqliteconnectionelectoralevent.InsertAll(parameters.Wards);
                sqliteconnectionelectoralevent.Close();

                FileInfo dbelectoraleventfileinfo = new (dbelectoraleventpath);

				string dbzipfilename = dbelectoraleventfileinfo.ZipFile();
				string dbgzipfilename = dbelectoraleventfileinfo.GZipFile();

				apifiles.Add(dbzipfilename.Split('\\').Last(), electoralevent);
				apifiles.Add(dbgzipfilename.Split('\\').Last(), electoralevent);

				File.Delete(dbelectoraleventpath);
                File.Delete(txtelectoraleventpath);
			}

			#endregion

			logfilestream.Close();

			parameters.SqliteConnection?.Close();
			parameters.SqliteConnectionProvinces?.Close();
			parameters.SqliteConnectionMunicipalities?.Close();

			FileInfo dbpathfileinfo = new(dbpath);

			string zipfilename = dbpathfileinfo.ZipFile();
            string gzipfilename = dbpathfileinfo.GZipFile();

			apifiles.Add(zipfilename.Split('\\').Last(), null);
			apifiles.Add(gzipfilename.Split('\\').Last(), null);

			string apifilesjson = apifiles.ToString();
			string apifilespath = Path.Combine(DirectoryOutput, "index.json");

			using FileStream apifilesfilestream = File.OpenWrite(apifilespath);
			using StreamWriter apifilesstreamwriter = new(apifilesfilestream);

			apifilesstreamwriter.Write(apifilesjson);
			apifilesstreamwriter.Close();
			apifilesfilestream.Close();

            new FileInfo(logpath).ZipFile();

			File.Delete(dbpath);
			File.Delete(logpath);
		}

        public static SQLiteConnection SQLiteConnection(string path)
        {
            SQLiteConnection sqliteconnection = new(path);

			sqliteconnection.CreateTable<Ballot>();
			sqliteconnection.CreateTable<ElectoralEvent>();
			sqliteconnection.CreateTable<Party>();
			sqliteconnection.CreateTable<VotingDistrict>();
			sqliteconnection.CreateTable<Ward>();

			return sqliteconnection;
        }

		public static void PksToText(string path, Parameters parameters)
        {
            if (File.Exists(path)) File.Delete(path);

            using FileStream filestream = File.Open(path, FileMode.CreateNew);
            using StreamWriter streamwriter = new(filestream);

            if (parameters.Parties is null)
                streamwriter.WriteLine(nameof(Parameters.Parties));
            else streamwriter.WriteLine(
                "{0},{1}",
                nameof(Parameters.Parties),
                string.Join(',', parameters.Parties.Select(party => party.Pk)));

            if (parameters.Provinces is null)
                streamwriter.WriteLine(nameof(Parameters.Provinces));
            else streamwriter.WriteLine(
                "{0},{1}",
                nameof(Parameters.Provinces),
                string.Join(',', parameters.Provinces.Select(province => province.Pk)));

            if (parameters.Municipalities is null)
                streamwriter.WriteLine(nameof(Parameters.Municipalities));
            else streamwriter.WriteLine(
                "{0},{1}",
                nameof(Parameters.Municipalities),
                string.Join(',', parameters.Municipalities.Select(municipality => municipality.Pk)));

            if (parameters.VotingDistricts is null)
                streamwriter.WriteLine(nameof(Parameters.VotingDistricts));
            else streamwriter.WriteLine(
                "{0},{1}",
                nameof(Parameters.VotingDistricts),
                string.Join(',', parameters.VotingDistricts.Select(votingdistrict => votingdistrict.Pk)));

            if (parameters.Wards is null)
                streamwriter.WriteLine(nameof(Parameters.Wards));
            else streamwriter.WriteLine(
                "{0},{1}",
                nameof(Parameters.Wards),
                string.Join(',', parameters.Wards.Select(ward => ward.Pk)));
        }
        public static string ElectoralEventPath(string basedirectory, ElectoralEvent electoralevent, string extension)
        {
            return ElectoralEventPath(basedirectory, electoralevent, extension, out string _);
        }
        public static string ElectoralEventPath(string basedirectory, ElectoralEvent electoralevent, string extension, out string filename)
        {
			return Path.Combine(basedirectory, filename = string.Format("iec.{0}.[{1}].{2}", electoralevent.Pk switch
            {
                01 => "NE.1994",
                02 => "PE.1994",
                03 => "NE.1999",
                04 => "PE.1999",
                05 => "LGE.2000",
                06 => "NE.2004",
                07 => "PE.2004",
                08 => "LGE.2006",
                09 => "NE.2009",
                10 => "PE.2009",
                11 => "LGE.2011",
                12 => "NE.2014",
                13 => "PE.2014",
                14 => "LGE.2016",
                15 => "NE.2019",
                16 => "PE.2019",
                17 => "LGE.2021",
                18 => "NE.2024",
                19 => "PE.2024",
                20 => "RE.2024",

                _ => throw new Exception(string.Format("Electoral Event PK '{0}' ?", electoralevent.Pk))

            }, electoralevent.Pk, extension));
        }
        public static void ElectoralEventLGEUpdate(SQLiteConnection sqliteconnection, ElectoralEvent electoralevent, IEnumerable<Ballot> ballots)
        {
            foreach (IGrouping<int?, Ballot> municipalityBallots in ballots.Where(ballot =>
                ballot.PkElectoralEvent == electoralevent.Pk &&
				ballot.PkProvince != null &&
				ballot.PkMunicipality != null &&
				ballot.PkWard == null &&
				ballot.PkVotingDistrict == null &&
				ballot.List_PkParty_Votes != null).GroupBy(ballot => ballot.PkMunicipality))
                if (municipalityBallots.Key.HasValue)
                {
                    Dictionary<int, int> List_PkParty_Votes = [];

                    foreach (int[] municipality_List_PkParty_Votes in municipalityBallots.SelectMany(municipalityBallot =>
                    {
                        return municipalityBallot.List_PkParty_Votes?
                            .Split(',')
                            .Select(_ =>
                            {
                                string[] split = _.Split(':');

                                return new int[]
                                {
                                    int.Parse(split[0]),
                                    int.Parse(split[1])
                                };

                            }) ?? Enumerable.Empty<int[]>();

                    })) 
                        if (List_PkParty_Votes.ContainsKey(municipality_List_PkParty_Votes[0]))
                            List_PkParty_Votes[municipality_List_PkParty_Votes[0]] += municipality_List_PkParty_Votes[1];
                        else List_PkParty_Votes.Add(municipality_List_PkParty_Votes[0], municipality_List_PkParty_Votes[1]);

                    if (List_PkParty_Votes.OrderByDescending(kvp => kvp.Value).Select(kvp => kvp.Key).FirstOrDefault() is int pkParty)
                        electoralevent.List_PkMunicipality_PkParty = _TableGeneral.AddPKPairIfUnique(
							electoralevent.List_PkMunicipality_PkParty, 
                            municipalityBallots.Key.Value, 
                            pkParty, 
                            false);
				}

            sqliteconnection.Update(electoralevent);
		}
        public static bool ChangeBallot<TCSVRow>(TCSVRow row, Ballot? ballot, string? votingdistrictid, string? wardid, string? municipalityvalue) where TCSVRow : Inputs.CSVs.CSVRow
        {
            if (ballot is null) return true;

            bool change = false;
			Inputs.CSVs.NPE1999? csvnpe1999 = row as Inputs.CSVs.NPE1999;
            Inputs.CSVs.CSVRowLGE? csvrowlge = row as Inputs.CSVs.CSVRowLGE;
            Inputs.CSVs.CSVRowNPE2? csvrownpe2 = row as Inputs.CSVs.CSVRowNPE2;

            if (csvrowlge is not null)
                change =
                    (ballot.Type is null && csvrowlge.BallotType is not null) ||
                    (ballot.Type is not null && csvrowlge.BallotType is null) ||
                    (string.Equals(ballot.Type, csvrowlge.BallotType) is false);

            if (change) return change;

            change =
                (votingdistrictid is null && row.VotingDistrictId is not null) ||
                (votingdistrictid is not null && row.VotingDistrictId is null) ||
                (Equals(votingdistrictid, row.VotingDistrictId) is false);

            if (change) return change;

            if (csvrowlge is not null)
                change =
                    (wardid is null && csvrowlge.WardId is not null) ||
                    (wardid is not null && csvrowlge.WardId is null) ||
                    (Equals(wardid, csvrowlge.WardId) is false);
            else if (csvrownpe2 is not null)
                change =
                    (wardid is null && csvrownpe2.WardId is not null) ||
                    (wardid is not null && csvrownpe2.WardId is null) ||
                    (Equals(wardid, csvrownpe2.WardId) is false);

            if (change) return change;

            if (csvnpe1999 is not null)
                change =
                    (municipalityvalue is null && csvnpe1999.MunicipalityName is not null) ||
                    (municipalityvalue is not null && csvnpe1999.MunicipalityName is null) ||
                    (string.Equals(municipalityvalue, csvnpe1999.MunicipalityName) is false);
            else change =
                    (municipalityvalue is null && row.MunicipalityGeo is not null) ||
                    (municipalityvalue is not null && row.MunicipalityGeo is null) ||
                    (string.Equals(municipalityvalue, row.MunicipalityGeo) is false);

            if (change) return change;

            change =
                (ballot.PkProvince is null && row.ProvincePk is not null) ||
                (ballot.PkProvince is not null && row.ProvincePk is null) ||
                (ballot.PkProvince != row.ProvincePk);

            return change;
        }
    }

	public static class Extensions
	{
		public static void Add(this JArray jarray, string filename, ElectoralEvent? electoralevent)
		{
			string description = filename.Split('.').Last() switch
			{
				"zip" => "zipped ",
				"gz" => "g-zipped ",

				_ => string.Empty
			};

			jarray.Add(new JObject
			{
				{ "DateCreated", DateTime.Now.ToString("dd-MM-yyyy") },
				{ "DateEdited", DateTime.Now.ToString("dd-MM-yyyy") },
				{ "Name", filename.Split('/').Last() },
				{ "Url", string.Format("https://raw.githubusercontent.com/xyclone-designs/database.iec/refs/heads/main/.output/{0}", filename) },
				{ "Description", true switch
					{
						true when electoralevent is not null
							=> string.Format("individual {0}database for {1} elections held on {2}", description, electoralevent.Type, electoralevent.Date),

						_ => string.Format("{0}database", description)
					}
				}
			});
		}
	}
}