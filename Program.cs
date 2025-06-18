using Database.IEC.Inputs.CSVs;

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

using _TableGeneral = XycloneDesigns.Apis.General.Tables._Table;
using static Database.IEC.Program;

namespace Database.IEC
{
    internal partial class Program
    {
		static readonly string DirectoryCurrent = Directory.GetCurrentDirectory();
		//static readonly string DirectoryCurrent = Directory.GetParent(Directory.GetCurrentDirectory())?.Parent?.Parent?.FullName!;

		static readonly string DirectoryOutput = Path.Combine(DirectoryCurrent, ".output");
		static readonly string DirectoryInput = Path.Combine(DirectoryCurrent, ".input");
		static readonly string DirectoryInputMunicipalities = Path.Combine(DirectoryInput, ".input");

		static void Main(string[] args)
		{
			if (Directory.Exists(DirectoryOutput) is false) Directory.CreateDirectory(DirectoryOutput);

			string dbname = "iec.db";
            string dbnameversioned = "iec.1.db";
            string dbmunicipalityname = "municipalities.db";
            string logname = "log.txt";
			string dbpath = Path.Combine(DirectoryOutput, dbname);
			string dbmunicipalitypath = Path.Combine(DirectoryOutput, dbmunicipalityname);
            string dbpathversioned = Path.Combine(DirectoryOutput, dbnameversioned);
            string logpath = Path.Combine(DirectoryOutput, logname);
            string datapath = Path.Combine(Directory.GetCurrentDirectory(), ".inputs", "data.zip");
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

            using FileStream datastream = File.OpenRead(datapath);
            using FileStream logfilestream = File.Open(logpath, FileMode.OpenOrCreate);
            using StreamWriter logstreamwriter = new(logfilestream);
            using ZipArchive datazip = new(datastream);
			using SQLiteConnection sqliteConnection = SQLiteConnection(dbpath);
			using SQLiteConnection sqliteconnectionmunicipality = new(dbmunicipalitypath);

			SQLiteConnection(dbpathversioned).Close();

			datazip.ReadDataProvinces(sqliteConnection, logstreamwriter);
            datazip.ReadDataParties(sqliteConnection, logstreamwriter);
			datazip.ReadDataAllocations(sqliteConnection, logstreamwriter);
			datazip.ReadDataMunicipalities(sqliteConnection, logstreamwriter);

            JArray apifiles = [];
            List<ElectoralEvent> electoralevents = [.. sqliteConnection.Table<ElectoralEvent>()];
			Parameters parameters = new()
            {
                ElectoralEvents = [],
                Parties = [],
                Provinces = [],
                Municipalities = [],
                VotingDistricts = [],
                Wards = [],
            };

            // 1994 National
            if (pksElectoralEvent.Contains(01))
            {
                Console.WriteLine("NE1994");

                ElectoralEvent NE1994ElectoralEvent = electoralevents.First(_ => NE1994.IsElectoralEvent(_));
                List<NE1994> NE1994Rows = NE1994.Rows().ToList();

				Console.WriteLine("NE1994 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, NE1994Rows);
                Console.WriteLine("NE1994 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, NE1994Rows);
                Console.WriteLine("NE1994 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, NE1994Rows);
                Console.WriteLine("NE1994 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, NE1994Rows);
                Console.WriteLine("NE1994 - CSV"); CSV(sqliteConnection, logstreamwriter, NE1994ElectoralEvent, NE1994Rows, parameters);

                NE1994Rows.Clear();

                IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == NE1994ElectoralEvent.Pk);

				string NE1994dbpath = ElectoralEventPath(DirectoryOutput, NE1994ElectoralEvent, "db");
                string NE1994txtpath = ElectoralEventPath(DirectoryOutput, NE1994ElectoralEvent, "txt");

                PksToText(NE1994txtpath, parameters);
                using SQLiteConnection connectionNE1994 = SQLiteConnection(NE1994dbpath);
                connectionNE1994.InsertAll(ballotsindividual);
                connectionNE1994.Close();   
			}

            // 1994 Provincial
            if (pksElectoralEvent.Contains(02))
            {
                Console.WriteLine("PE1994");

                ElectoralEvent PE1994ElectoralEvent = electoralevents.First(_ => PE1994.IsElectoralEvent(_));
                List<PE1994> PE1994Rows = PE1994.Rows().ToList();

                Console.WriteLine("PE1994 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, PE1994Rows);
                Console.WriteLine("PE1994 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, PE1994Rows);
                Console.WriteLine("PE1994 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, PE1994Rows);
                Console.WriteLine("PE1994 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, PE1994Rows);
                Console.WriteLine("PE1994 - CSV"); CSV(sqliteConnection, logstreamwriter, PE1994ElectoralEvent, PE1994Rows, parameters);

                PE1994Rows.Clear();

                IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == PE1994ElectoralEvent.Pk);

                string PE1994dbpath = ElectoralEventPath(DirectoryOutput, PE1994ElectoralEvent, "db");
                string PE1994txtpath = ElectoralEventPath(DirectoryOutput, PE1994ElectoralEvent, "txt");

                PksToText(PE1994txtpath, parameters);
                using SQLiteConnection connectionPE1994 = SQLiteConnection(PE1994dbpath);
                connectionPE1994.InsertAll(ballotsindividual);
                connectionPE1994.Close();
            }

            // 1999 National & Provincial
            if (pksElectoralEvent.Contains(03) is bool NE1999 && pksElectoralEvent.Contains(04) is bool PE1999 && (NE1999 || PE1999))
            {
                Console.WriteLine("NPE1999");

                ElectoralEvent
                    NE1999ElectoralEvent = electoralevents.First(_ => NPE1999.IsElectoralEventNE(_)),
                    PE1999ElectoralEvent = electoralevents.First(_ => NPE1999.IsElectoralEventPE(_));
                List<NPE1999> NE1999Rows = [], PE1999Rows = [];

                foreach (NPE1999 row in UtilsCSVRows<NPE1999>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("1999 NPE.csv")).Open()))
                    if (NE1999 && row.GetBallotType() == ElectoralEvent.Types.National)
                        NE1999Rows.Add(row);
                    else if (PE1999 && row.GetBallotType() == ElectoralEvent.Types.Provincial)
                        PE1999Rows.Add(row);

                Console.WriteLine("NPE1999 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, NE1999Rows);
                Console.WriteLine("NPE1999 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, NE1999Rows);
                Console.WriteLine("NPE1999 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, NE1999Rows);
                Console.WriteLine("NPE1999 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, NE1999Rows);
                if (NE1999) Console.WriteLine("NPE1999 - CSV National"); CSV(sqliteConnection, logstreamwriter, NE1999ElectoralEvent, NE1999Rows, parameters);
                if (PE1999) Console.WriteLine("NPE1999 - CSV Provincial"); CSV(sqliteConnection, logstreamwriter, PE1999ElectoralEvent, PE1999Rows, parameters);

                NE1999Rows.Clear();
                PE1999Rows.Clear();

                if (NE1999)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == NE1999ElectoralEvent.Pk);

                    string NE1999dbpath = ElectoralEventPath(DirectoryOutput, NE1999ElectoralEvent, "db");
                    string NE1999txtpath = ElectoralEventPath(DirectoryOutput, NE1999ElectoralEvent, "txt");
                    PksToText(NE1999txtpath, parameters);
                    using SQLiteConnection connectionNE1999 = SQLiteConnection(NE1999dbpath);
                    connectionNE1999.InsertAll(ballotsindividual);
                    connectionNE1999.Close();
                }
                if (PE1999)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == PE1999ElectoralEvent.Pk);

                    string PE1999dbpath = ElectoralEventPath(DirectoryOutput, PE1999ElectoralEvent, "db");
                    string PE1999txtpath = ElectoralEventPath(DirectoryOutput, PE1999ElectoralEvent, "txt");
                    PksToText(PE1999txtpath, parameters);
                    using SQLiteConnection connectionPE1999 = SQLiteConnection(PE1999dbpath);
                    connectionPE1999.InsertAll(ballotsindividual);
                    connectionPE1999.Close();
                }
            }

            // 2000 Municipal
            if (pksElectoralEvent.Contains(05))
            {
                Console.WriteLine("LGE2000");

                ElectoralEvent LGE2000ElectoralEvent = electoralevents.First(_ => LGE2000.IsElectoralEvent(_));
                List<LGE2000> LGE2000Rows = UtilsCSVRows<LGE2000>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2000 LGE.csv")).Open()).ToList();
                List<Inputs.XLSs.LGE2000> LGE2000Seats = UtilsXLSSeats<Inputs.XLSs.LGE2000>(datazip, "LGE2000", dataset => new Inputs.XLSs.LGE2000(dataset)).ToList();

				Console.WriteLine("LGE2000 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, LGE2000Rows);
                Console.WriteLine("LGE2000 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, LGE2000Rows);
                Console.WriteLine("LGE2000 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, LGE2000Rows);
                Console.WriteLine("LGE2000 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, LGE2000Rows);
                Console.WriteLine("LGE2000 - CSV"); CSV(sqliteConnection, logstreamwriter, LGE2000ElectoralEvent, LGE2000Rows, parameters);

				LGE2000Rows.Clear();

                XLS<Inputs.XLSs.LGE2000, Inputs.XLSs.LGE2000.Row>(sqliteConnection, LGE2000ElectoralEvent, LGE2000Seats);

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == LGE2000ElectoralEvent.Pk);

                string LGE2000dbpath = ElectoralEventPath(DirectoryOutput, LGE2000ElectoralEvent, "db");
                string LGE2000txtpath = ElectoralEventPath(DirectoryOutput, LGE2000ElectoralEvent, "txt");

                PksToText(LGE2000txtpath, parameters);
                using SQLiteConnection connectionLGE2000 = SQLiteConnection(LGE2000dbpath);
                connectionLGE2000.InsertAll(ballotsindividual);
                connectionLGE2000.Close();
            }

            // 2004 National & Provincial
            if (pksElectoralEvent.Contains(06) is bool NE2004 && pksElectoralEvent.Contains(07) is bool PE2004 && (NE2004 || PE2004))
            {
                Console.WriteLine("NPE2004");

                ElectoralEvent
                    NE2004ElectoralEvent = electoralevents.First(_ => NPE2004.IsElectoralEventNE(_)),
                    PE2004ElectoralEvent = electoralevents.First(_ => NPE2004.IsElectoralEventPE(_));
                List<NPE2004> NE2004Rows = [], PE2004Rows = [];

                foreach (NPE2004 row in UtilsCSVRows<NPE2004>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2004 NPE.csv")).Open()))
                    if (NE2004 && row.GetBallotType() == ElectoralEvent.Types.National)
                        NE2004Rows.Add(row);
                    else if (PE2004 && row.GetBallotType() == ElectoralEvent.Types.Provincial)
                        PE2004Rows.Add(row);

                Console.WriteLine("NPE2004 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, NE2004Rows);
                Console.WriteLine("NPE2004 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, NE2004Rows);
                Console.WriteLine("NPE2004 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, NE2004Rows);
                Console.WriteLine("NPE2004 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, NE2004Rows);
                if (NE2004) Console.WriteLine("NPE2004 - CSV National"); CSV(sqliteConnection, logstreamwriter, NE2004ElectoralEvent, NE2004Rows, parameters);
                if (PE2004) Console.WriteLine("NPE2004 - CSV Provincial"); CSV(sqliteConnection, logstreamwriter, PE2004ElectoralEvent, PE2004Rows, parameters);

                NE2004Rows.Clear();
                PE2004Rows.Clear();

                if (NE2004)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == NE2004ElectoralEvent.Pk);

                    string NE2004dbpath = ElectoralEventPath(DirectoryOutput, NE2004ElectoralEvent, "db");
                    string NE2004txtpath = ElectoralEventPath(DirectoryOutput, NE2004ElectoralEvent, "txt");
                    PksToText(NE2004txtpath, parameters);
                    using SQLiteConnection connectionNE2004 = SQLiteConnection(NE2004dbpath);
                    connectionNE2004.InsertAll(ballotsindividual);
                    connectionNE2004.Close();
                }
                if (PE2004)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == PE2004ElectoralEvent.Pk);

                    string PE2004dbpath = ElectoralEventPath(DirectoryOutput, PE2004ElectoralEvent, "db");
                    string PE2004txtpath = ElectoralEventPath(DirectoryOutput, PE2004ElectoralEvent, "txt");
                    PksToText(PE2004txtpath, parameters);
                    using SQLiteConnection connectionPE2004 = SQLiteConnection(PE2004dbpath);
                    connectionPE2004.InsertAll(ballotsindividual);
                    connectionPE2004.Close();
                }
            }

            // 2006 Municipal
            if (pksElectoralEvent.Contains(08))
            {
                Console.WriteLine("LGE2006");

                ElectoralEvent LGE2006ElectoralEvent = electoralevents.First(_ => LGE2006.IsElectoralEvent(_));
                List<LGE2006> LGE2006Rows = UtilsCSVRows<LGE2006>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2006 LGE.csv")).Open()).ToList();
				List<Inputs.XLSs.LGE2006> LGE2006Seats = UtilsXLSSeats<Inputs.XLSs.LGE2006>(datazip, "LGE2006", dataset => new Inputs.XLSs.LGE2006(dataset)).ToList();

				Console.WriteLine("LGE2006 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, LGE2006Rows);
                Console.WriteLine("LGE2006 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, LGE2006Rows);
                Console.WriteLine("LGE2006 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, LGE2006Rows);
                Console.WriteLine("LGE2006 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, LGE2006Rows);
                Console.WriteLine("LGE2006 - CSV"); CSV(sqliteConnection, logstreamwriter, LGE2006ElectoralEvent, LGE2006Rows, parameters);

                LGE2006Rows.Clear();

				XLS<Inputs.XLSs.LGE2006, Inputs.XLSs.LGE2006.Row>(sqliteConnection, LGE2006ElectoralEvent, LGE2006Seats);

                ElectoralEventLGEUpdate(sqliteConnection, LGE2006ElectoralEvent, sqliteConnection.Table<Ballot>());

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == LGE2006ElectoralEvent.Pk);

                string LGE2006dbpath = ElectoralEventPath(DirectoryOutput, LGE2006ElectoralEvent, "db");
                string LGE2006txtpath = ElectoralEventPath(DirectoryOutput, LGE2006ElectoralEvent, "txt");

                PksToText(LGE2006txtpath, parameters);
                using SQLiteConnection connectionLGE2006 = SQLiteConnection(LGE2006dbpath);
                connectionLGE2006.InsertAll(ballotsindividual);
                connectionLGE2006.Close();
            }

            // 2009 National & Provincial
            if (pksElectoralEvent.Contains(09) is bool NE2009 && pksElectoralEvent.Contains(10) is bool PE2009 && (NE2009 || PE2009))
            {
                Console.WriteLine("NPE2009");

                ElectoralEvent
                    NE2009ElectoralEvent = electoralevents.First(_ => NPE2009.IsElectoralEventNE(_)),
                    PE2009ElectoralEvent = electoralevents.First(_ => NPE2009.IsElectoralEventPE(_));
                List<NPE2009> NE2009Rows = [], PE2009Rows = [];

                foreach (NPE2009 row in UtilsCSVRows<NPE2009>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2009 NPE.csv")).Open()))
                    if (NE2009 && row.GetBallotType() == ElectoralEvent.Types.National)
                        NE2009Rows.Add(row);
                    else if (PE2009 && row.GetBallotType() == ElectoralEvent.Types.Provincial)
                        PE2009Rows.Add(row);

                Console.WriteLine("NPE2009 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, NE2009Rows);
                Console.WriteLine("NPE2009 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, NE2009Rows);
                Console.WriteLine("NPE2009 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, NE2009Rows);
                Console.WriteLine("NPE2009 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, NE2009Rows);
                if (NE2009) Console.WriteLine("NPE2009 - CSV National"); CSV(sqliteConnection, logstreamwriter, NE2009ElectoralEvent, NE2009Rows, parameters);
                if (PE2009) Console.WriteLine("NPE2009 - CSV Provincial"); CSV(sqliteConnection, logstreamwriter, PE2009ElectoralEvent, PE2009Rows, parameters);

                NE2009Rows.Clear();
                PE2009Rows.Clear();

                if (NE2009)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == NE2009ElectoralEvent.Pk);

                    string NE2009dbpath = ElectoralEventPath(DirectoryOutput, NE2009ElectoralEvent, "db");
                    string NE2009txtpath = ElectoralEventPath(DirectoryOutput, NE2009ElectoralEvent, "txt");
                    PksToText(NE2009txtpath, parameters);
                    using SQLiteConnection connectionNE2009 = SQLiteConnection(NE2009dbpath);
                    connectionNE2009.InsertAll(ballotsindividual);
                    connectionNE2009.Close();
                }
                if (PE2009)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == PE2009ElectoralEvent.Pk);

                    string PE2009dbpath = ElectoralEventPath(DirectoryOutput, PE2009ElectoralEvent, "db");
                    string PE2009txtpath = ElectoralEventPath(DirectoryOutput, PE2009ElectoralEvent, "txt");
                    PksToText(PE2009txtpath, parameters);
                    using SQLiteConnection connectionPE2009 = SQLiteConnection(PE2009dbpath);
                    connectionPE2009.InsertAll(ballotsindividual);
                    connectionPE2009.Close();
                }
            }

            // 2011 Municipal
            if (pksElectoralEvent.Contains(11))
            {
                Console.WriteLine("LGE2011");

                ElectoralEvent LGE2011ElectoralEvent = electoralevents.First(_ => LGE2011.IsElectoralEvent(_));
                List<LGE2011> LGE2011Rows = UtilsCSVRows<LGE2011>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2011 LGE.csv")).Open()).ToList();
				List<Inputs.XLSs.LGE2011> LGE2011Seats = UtilsXLSSeats<Inputs.XLSs.LGE2011>(datazip, "LGE2011", dataset => new Inputs.XLSs.LGE2011(dataset)).ToList();

				Console.WriteLine("LGE2011 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, LGE2011Rows);
                Console.WriteLine("LGE2011 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, LGE2011Rows);
                Console.WriteLine("LGE2011 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, LGE2011Rows);
                Console.WriteLine("LGE2011 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, LGE2011Rows);
                Console.WriteLine("LGE2011 - CSV"); CSV(sqliteConnection, logstreamwriter, LGE2011ElectoralEvent, LGE2011Rows, parameters);

                LGE2011Rows.Clear();

				XLS<Inputs.XLSs.LGE2011, Inputs.XLSs.LGE2011.Row>(sqliteConnection, LGE2011ElectoralEvent, LGE2011Seats);

				ElectoralEventLGEUpdate(sqliteConnection, LGE2011ElectoralEvent, sqliteConnection.Table<Ballot>());

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == LGE2011ElectoralEvent.Pk);

                string LGE2011dbpath = ElectoralEventPath(DirectoryOutput, LGE2011ElectoralEvent, "db");
                string LGE2011txtpath = ElectoralEventPath(DirectoryOutput, LGE2011ElectoralEvent, "txt");

                PksToText(LGE2011txtpath, parameters);
                using SQLiteConnection connectionLGE2011 = SQLiteConnection(LGE2011dbpath);
                connectionLGE2011.InsertAll(ballotsindividual);
                connectionLGE2011.Close();
            }

            // 2014 National & Provincial
            if (pksElectoralEvent.Contains(12) is bool NE2014 && pksElectoralEvent.Contains(13) is bool PE2014 && (NE2014 || PE2014))
            {
                Console.WriteLine("NPE2014");

                ElectoralEvent
                    NE2014ElectoralEvent = electoralevents.First(_ => NPE2014.IsElectoralEventNE(_)),
                    PE2014ElectoralEvent = electoralevents.First(_ => NPE2014.IsElectoralEventPE(_));
                List<NPE2014> NE2014Rows = [], PE2014Rows = [];

                foreach (NPE2014 row in UtilsCSVRows<NPE2014>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2014 NPE.csv")).Open()))
                    if (NE2014 && row.GetBallotType() == ElectoralEvent.Types.National)
                        NE2014Rows.Add(row);
                    else if (PE2014 && row.GetBallotType() == ElectoralEvent.Types.Provincial)
                        PE2014Rows.Add(row);

                Console.WriteLine("NPE2014 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, NE2014Rows);
                Console.WriteLine("NPE2014 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, NE2014Rows);
                Console.WriteLine("NPE2014 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, NE2014Rows);
                Console.WriteLine("NPE2014 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, NE2014Rows);
                if (NE2014) Console.WriteLine("NPE2014 - CSV National"); CSV(sqliteConnection, logstreamwriter, NE2014ElectoralEvent, NE2014Rows, parameters);
                if (PE2014) Console.WriteLine("NPE2014 - CSV Provincial"); CSV(sqliteConnection, logstreamwriter, PE2014ElectoralEvent, PE2014Rows, parameters);

                NE2014Rows.Clear();
                PE2014Rows.Clear();

                if (NE2014)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == NE2014ElectoralEvent.Pk);

                    string NE2014dbpath = ElectoralEventPath(DirectoryOutput, NE2014ElectoralEvent, "db");
                    string NE2014txtpath = ElectoralEventPath(DirectoryOutput, NE2014ElectoralEvent, "txt");
                    PksToText(NE2014txtpath, parameters);
                    using SQLiteConnection connectionNE2014 = SQLiteConnection(NE2014dbpath);
                    connectionNE2014.InsertAll(ballotsindividual);
                    connectionNE2014.Close();
                }
                if (PE2014)
                {
                    IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                        .Where(ballot => ballot.PkElectoralEvent == PE2014ElectoralEvent.Pk);

                    string PE2014dbpath = ElectoralEventPath(DirectoryOutput, PE2014ElectoralEvent, "db");
                    string PE2014txtpath = ElectoralEventPath(DirectoryOutput, PE2014ElectoralEvent, "txt");
                    PksToText(PE2014txtpath, parameters);
                    using SQLiteConnection connectionPE2014 = SQLiteConnection(PE2014dbpath);
                    connectionPE2014.InsertAll(ballotsindividual);
                    connectionPE2014.Close();
                }
            }

            // 2016 Municipal
            if (pksElectoralEvent.Contains(14))
            {
                Console.WriteLine("LGE2016");

                ElectoralEvent LGE2016ElectoralEvent = electoralevents.First(_ => LGE2016.IsElectoralEvent(_));
                List<LGE2016> LGE2016Rows = UtilsCSVRows<LGE2016>(
                    logstreamwriter,
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE EC.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE FS.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE GP.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE KZN.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE LIM.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE MP.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE NC.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE NW.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE WP.csv")).Open()).ToList();
				List<Inputs.XLSs.LGE2016> LGE2016Seats = UtilsXLSSeats<Inputs.XLSs.LGE2016>(datazip, "LGE2016", dataset => new Inputs.XLSs.LGE2016(dataset)).ToList();

				Console.WriteLine("LGE2016 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, LGE2016Rows);
                Console.WriteLine("LGE2016 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, LGE2016Rows);
                Console.WriteLine("LGE2016 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, LGE2016Rows);
                Console.WriteLine("LGE2016 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, LGE2016Rows);
                Console.WriteLine("LGE2016 - CSV"); CSV(sqliteConnection, logstreamwriter, LGE2016ElectoralEvent, LGE2016Rows, parameters);

                LGE2016Rows.Clear();

				XLS<Inputs.XLSs.LGE2016, Inputs.XLSs.LGE2016.Row>(sqliteConnection, LGE2016ElectoralEvent, LGE2016Seats);

				ElectoralEventLGEUpdate(sqliteConnection, LGE2016ElectoralEvent, sqliteConnection.Table<Ballot>());

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == LGE2016ElectoralEvent.Pk);

                string LGE2016dbpath = ElectoralEventPath(DirectoryOutput, LGE2016ElectoralEvent, "db");
                string LGE2016txtpath = ElectoralEventPath(DirectoryOutput, LGE2016ElectoralEvent, "txt");

                PksToText(LGE2016txtpath, parameters);
                using SQLiteConnection connectionLGE2016 = SQLiteConnection(LGE2016dbpath);
                connectionLGE2016.InsertAll(ballotsindividual);
                connectionLGE2016.Close();
            }

            // 2019 National
            if (pksElectoralEvent.Contains(15))
            {
                Console.WriteLine("NE2019");

                ElectoralEvent NE2019ElectoralEvent = electoralevents.First(_ => NE2019.IsElectoralEvent(_));
                List<NE2019> NE2019Rows = UtilsCSVRows<NE2019>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2019 NE.csv")).Open())
                    .Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
                    .ToList();

                Console.WriteLine("NE2019 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, NE2019Rows);
                Console.WriteLine("NE2019 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, NE2019Rows);
                Console.WriteLine("NE2019 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, NE2019Rows);
                Console.WriteLine("NE2019 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, NE2019Rows);
                Console.WriteLine("NE2019 - CSV"); CSV(sqliteConnection, logstreamwriter, NE2019ElectoralEvent, NE2019Rows, parameters);

                NE2019Rows.Clear();

                IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == NE2019ElectoralEvent.Pk);

                string NE2019dbpath = ElectoralEventPath(DirectoryOutput, NE2019ElectoralEvent, "db");
                string NE2019txtpath = ElectoralEventPath(DirectoryOutput, NE2019ElectoralEvent, "txt");

                PksToText(NE2019txtpath, parameters);
                using SQLiteConnection connectionNE2019 = SQLiteConnection(NE2019dbpath);
                connectionNE2019.InsertAll(ballotsindividual);
                connectionNE2019.Close();
            }

            // 2019 Provincial
            if (pksElectoralEvent.Contains(16))
            {
                Console.WriteLine("PE2019");

                ElectoralEvent PE2019ElectoralEvent = electoralevents.First(_ => PE2019.IsElectoralEvent(_));
                List<PE2019> PE2019Rows = UtilsCSVRows<PE2019>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2019 PE.csv")).Open())
                    .Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
                    .ToList();

                Console.WriteLine("PE2019 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, PE2019Rows);
                Console.WriteLine("PE2019 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, PE2019Rows);
                Console.WriteLine("PE2019 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, PE2019Rows);
                Console.WriteLine("PE2019 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, PE2019Rows);
                Console.WriteLine("PE2019 - CSV"); CSV(sqliteConnection, logstreamwriter, PE2019ElectoralEvent, PE2019Rows, parameters);

                PE2019Rows.Clear();

                IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == PE2019ElectoralEvent.Pk);

                string PE2019dbpath = ElectoralEventPath(DirectoryOutput, PE2019ElectoralEvent, "db");
                string PE2019txtpath = ElectoralEventPath(DirectoryOutput, PE2019ElectoralEvent, "txt");

                PksToText(PE2019txtpath, parameters);
                using SQLiteConnection connectionPE2019 = SQLiteConnection(PE2019dbpath);
                connectionPE2019.InsertAll(ballotsindividual);
                connectionPE2019.Close();
            }

            // 2021 Municipal
            if (pksElectoralEvent.Contains(17))
            {
                Console.WriteLine("LGE2021");

                ElectoralEvent LGE2021ElectoralEvent = electoralevents.First(_ => LGE2021.IsElectoralEvent(_));
                List<LGE2021> LGE2021Rows = UtilsCSVRows<LGE2021>(
                    logstreamwriter,
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE EC.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE FS.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE GP.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE KZN.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE LIM.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE MP.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE NC.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE NW.csv")).Open(),
                    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE WP.csv")).Open()).ToList();
				List<Inputs.XLSs.LGE2021> LGE2021Seats = UtilsXLSSeats<Inputs.XLSs.LGE2021>(datazip, "LGE2021", dataset => new Inputs.XLSs.LGE2021(dataset)).ToList();

				Console.WriteLine("LGE2021 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, LGE2021Rows);
                Console.WriteLine("LGE2021 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, LGE2021Rows);
                Console.WriteLine("LGE2021 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, LGE2021Rows);
                Console.WriteLine("LGE2021 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, LGE2021Rows);
                Console.WriteLine("LGE2021 - CSV"); CSV(sqliteConnection, logstreamwriter, LGE2021ElectoralEvent, LGE2021Rows, parameters);

                LGE2021Rows.Clear();

				XLS<Inputs.XLSs.LGE2021, Inputs.XLSs.LGE2021.Row>(sqliteConnection, LGE2021ElectoralEvent, LGE2021Seats);

				ElectoralEventLGEUpdate(sqliteConnection, LGE2021ElectoralEvent, sqliteConnection.Table<Ballot>());

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
                    .Where(ballot => ballot.PkElectoralEvent == LGE2021ElectoralEvent.Pk);

                string LGE2021dbpath = ElectoralEventPath(DirectoryOutput, LGE2021ElectoralEvent, "db");
                string LGE2021txtpath = ElectoralEventPath(DirectoryOutput, LGE2021ElectoralEvent, "txt");

                PksToText(LGE2021txtpath, parameters);
                using SQLiteConnection connectionLGE2021 = SQLiteConnection(LGE2021dbpath);
                connectionLGE2021.InsertAll(ballotsindividual);
                connectionLGE2021.Close();
            }

			// 2024 National
			if (pksElectoralEvent.Contains(18))
			{
				Console.WriteLine("NE2024");

				ElectoralEvent NE2024ElectoralEvent = electoralevents.First(_ => NE2024.IsElectoralEvent(_));
				List<NE2024> NE2024Rows = UtilsCSVRows<NE2024>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2024 NE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList();

				Console.WriteLine("NE2024 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, NE2024Rows);
				Console.WriteLine("NE2024 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, NE2024Rows);
				Console.WriteLine("NE2024 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, NE2024Rows);
				Console.WriteLine("NE2024 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, NE2024Rows);
				Console.WriteLine("NE2024 - CSV"); CSV(sqliteConnection, logstreamwriter, NE2024ElectoralEvent, NE2024Rows, parameters);

				NE2024Rows.Clear();

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
					.Where(ballot => ballot.PkElectoralEvent == NE2024ElectoralEvent.Pk);

				string NE2024dbpath = ElectoralEventPath(DirectoryOutput, NE2024ElectoralEvent, "db");
				string NE2024txtpath = ElectoralEventPath(DirectoryOutput, NE2024ElectoralEvent, "txt");

				PksToText(NE2024txtpath, parameters);
				using SQLiteConnection connectionNE2024 = SQLiteConnection(NE2024dbpath);
				connectionNE2024.InsertAll(ballotsindividual);
				connectionNE2024.Close();
			}

			// 2024 Provincial
			if (pksElectoralEvent.Contains(19))
			{
				Console.WriteLine("PE2024");

				ElectoralEvent PE2024ElectoralEvent = electoralevents.First(_ => PE2024.IsElectoralEvent(_));
				List<PE2024> PE2024Rows = UtilsCSVRows<PE2024>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2024 PE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList();

				Console.WriteLine("PE2024 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, PE2024Rows);
				Console.WriteLine("PE2024 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, PE2024Rows);
				Console.WriteLine("PE2024 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, PE2024Rows);
				Console.WriteLine("PE2024 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, PE2024Rows);
				Console.WriteLine("PE2024 - CSV"); CSV(sqliteConnection, logstreamwriter, PE2024ElectoralEvent, PE2024Rows, parameters);

				PE2024Rows.Clear();

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
					.Where(ballot => ballot.PkElectoralEvent == PE2024ElectoralEvent.Pk);

				string PE2024dbpath = ElectoralEventPath(DirectoryOutput, PE2024ElectoralEvent, "db");
				string PE2024txtpath = ElectoralEventPath(DirectoryOutput, PE2024ElectoralEvent, "txt");

				PksToText(PE2024txtpath, parameters);
				using SQLiteConnection connectionPE2024 = SQLiteConnection(PE2024dbpath);
				connectionPE2024.InsertAll(ballotsindividual);
				connectionPE2024.Close();
			}

			// 2024 Regional
			if (pksElectoralEvent.Contains(20))
			{
				Console.WriteLine("RE2024");

				ElectoralEvent RE2024ElectoralEvent = electoralevents.First(_ => RE2024.IsElectoralEvent(_));
				List<RE2024> RE2024Rows = UtilsCSVRows<RE2024>(logstreamwriter, datazip.Entries.First(entry => entry.FullName.EndsWith("2024 RE.csv")).Open())
					.Where(_ => _.Line is null || _.Line.Contains("Out of Country") is false)
					.ToList();

				Console.WriteLine("RE2024 - New Parties"); parameters.Parties = sqliteConnection.CSVNewParties(logstreamwriter, RE2024Rows);
				Console.WriteLine("RE2024 - New Municipalities"); parameters.Municipalities = sqliteConnection.CSVNewMunicipalities(logstreamwriter, RE2024Rows);
				Console.WriteLine("RE2024 - New Voting Districts"); parameters.VotingDistricts = sqliteConnection.CSVNewVotingDistricts(logstreamwriter, RE2024Rows);
				Console.WriteLine("RE2024 - New Wards"); parameters.Wards = sqliteConnection.CSVNewWards(logstreamwriter, RE2024Rows);
				Console.WriteLine("RE2024 - CSV"); CSV(sqliteConnection, logstreamwriter, RE2024ElectoralEvent, RE2024Rows, parameters);

				RE2024Rows.Clear();

				IEnumerable<Ballot> ballotsindividual = sqliteConnection.Table<Ballot>()
					.Where(ballot => ballot.PkElectoralEvent == RE2024ElectoralEvent.Pk);

				string RE2024dbpath = ElectoralEventPath(DirectoryOutput, RE2024ElectoralEvent, "db");
				string RE2024txtpath = ElectoralEventPath(DirectoryOutput, RE2024ElectoralEvent, "txt");

				PksToText(RE2024txtpath, parameters);
				using SQLiteConnection connectionRE2024 = SQLiteConnection(RE2024dbpath);
				connectionRE2024.InsertAll(ballotsindividual);
				connectionRE2024.Close();
			}

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
                            case nameof(Parameters.Parties):
                                parameters.Parties = ((IEnumerable<Party>)sqliteConnection.Table<Party>())
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.Provinces):
                                parameters.Provinces = ((IEnumerable<Province>)sqliteConnection.Table<Province>())
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.Municipalities):
                                parameters.Municipalities = ((IEnumerable<Municipality>)sqliteConnection.Table<Municipality>())
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.VotingDistricts):
                                parameters.VotingDistricts = ((IEnumerable<VotingDistrict>)sqliteConnection.Table<VotingDistrict>())
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            case nameof(Parameters.Wards):
                                parameters.Wards = ((IEnumerable<Ward>)sqliteConnection.Table<Ward>())
                                    .Where(_ => values[1..^0].Contains(_.Pk.ToString()))
                                    .ToList();
                                break;

                            default: break;
                        }
                }

                txtelectoraleventfilestream.Close();
                sqliteconnectionelectoralevent.Insert(electoralevent);
                sqliteconnectionelectoralevent.InsertAll(parameters.Municipalities);
                sqliteconnectionelectoralevent.InsertAll(parameters.Provinces);
                sqliteconnectionelectoralevent.InsertAll(parameters.Parties);
                sqliteconnectionelectoralevent.InsertAll(parameters.VotingDistricts);
                sqliteconnectionelectoralevent.InsertAll(parameters.Wards);
                sqliteconnectionelectoralevent.Close();

                string dbzipfilename = ZipFile(dbelectoraleventpath);
				string dbgzipfilename = GZipFile(dbelectoraleventpath);

				apifiles.Add(dbzipfilename.Split('\\').Last(), electoralevent);
				apifiles.Add(dbgzipfilename.Split('\\').Last(), electoralevent);

				File.Delete(dbelectoraleventpath);
                File.Delete(txtelectoraleventpath);
			}

			#endregion

			logfilestream.Close();
			sqliteConnection.Close();

			string zipfilename = ZipFile(dbpath);
            string gzipfilename = GZipFile(dbpath);

			apifiles.Add(zipfilename.Split('\\').Last(), null);
			apifiles.Add(gzipfilename.Split('\\').Last(), null);

			string apifilesjson = apifiles.ToString();
			string apifilespath = Path.Combine(directory, "index.json");

			using FileStream apifilesfilestream = File.OpenWrite(apifilespath);
			using StreamWriter apifilesstreamwriter = new(apifilesfilestream);

			apifilesstreamwriter.Write(apifilesjson);
			apifilesstreamwriter.Close();
			apifilesfilestream.Close();

			ZipFile(logpath);

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
        public static string ZipFile(string filepath)
        {
            string name = filepath.Split("\\").Last();
            string zipfilepath = filepath + ".zip";

            using FileStream filestream = File.OpenRead(filepath);
            using FileStream filestreamzip = File.Create(zipfilepath);
            using ZipArchive ziparchive = new(filestreamzip, ZipArchiveMode.Create, true);
            using Stream stream = ziparchive
                .CreateEntry(name)
                .Open();

            filestream.CopyTo(stream);
            filestream.Close();

            return zipfilepath;
        }
		public static string GZipFile(string filepath)
		{
			string gzipfilepath = filepath + ".gz";

			using FileStream filestream = File.OpenRead(filepath);
			using FileStream filestreamgzip = File.Create(gzipfilepath);
			
            GZip.Compress(filestream, filestreamgzip, true, 512, 6);

			return gzipfilepath;
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
        public static bool ChangeBallot<TCSVRow>(TCSVRow row, Ballot? ballot, string? votingdistrictid, string? wardid, string? municipalityvalue) where TCSVRow : CSVRow
        {
            if (ballot is null) return true;

            bool change = false;
            NPE1999? csvnpe1999 = row as NPE1999;
            CSVRowLGE? csvrowlge = row as CSVRowLGE;
            CSVRowNPE2? csvrownpe2 = row as CSVRowNPE2;

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