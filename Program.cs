using DataProcessor.CSVs;
using DataProcessor.Tables;

using SQLite;

using System.IO.Compression;

namespace DataProcessor
{
    internal partial class Program
    {
        static void Main(string[] args)
        {
            string dbname = "iec.db";
            string directory = Directory.GetCurrentDirectory();
            string dbpath = Path.Combine(directory, dbname);
            string dbzippath = Path.Combine(directory, "iec.zip");
            string logpath = Path.Combine(directory, "log.txt");
            string datapath = Path.Combine(directory, "data.zip");

            if (File.Exists(dbpath)) File.Delete(dbpath);
            if (File.Exists(dbzippath)) File.Delete(dbzippath);
            if (File.Exists(logpath)) File.Delete(logpath);

            using FileStream datastream = File.OpenRead(datapath);
            using StreamWriter logger = File.AppendText(logpath);
            using ZipArchive datazip = new(datastream);
            using SQLiteConnection sqliteConnection = new(dbpath);

            sqliteConnection.CreateTable<Ballot>();
            sqliteConnection.CreateTable<ElectoralEvent>();
            sqliteConnection.CreateTable<Municipality>();
            sqliteConnection.CreateTable<Party>();
            sqliteConnection.CreateTable<Province>();
            sqliteConnection.CreateTable<VotingDistrict>();
            sqliteConnection.CreateTable<Ward>();

            ReadDataProvinces(sqliteConnection, datazip, logger);
            ReadDataParties(sqliteConnection, datazip, logger);
            ReadDataAllocations(sqliteConnection, datazip, logger);
            ReadDataMunicipalities(sqliteConnection, datazip, logger);

            List<Party> parties = [];
            List<Municipality> municipalities = [];
            List<VotingDistrict> votingDistrict = [];
            List<Ward> wards = [];
            List<ElectoralEvent> electoralevents = [.. sqliteConnection.Table<ElectoralEvent>()];

            #region 1994 National

            Console.WriteLine("NE1994");

            List<NE1994> NE1994Rows = NE1994.Rows().ToList();

            Console.WriteLine("NE1994 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, NE1994Rows);
            Console.WriteLine("NE1994 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, NE1994Rows);
            Console.WriteLine("NE1994 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, NE1994Rows);
            Console.WriteLine("NE1994 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, NE1994Rows);

            Console.WriteLine("NE1994"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NE1994.IsElectoralEvent(_)),
                NE1994Rows,
                parties, municipalities, votingDistrict, wards);

            NE1994Rows.Clear();

            #endregion

            #region 1994 Provincial

            Console.WriteLine("PE1994");

            List<PE1994> PE1994Rows = PE1994.Rows().ToList();

            Console.WriteLine("PE1994 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, PE1994Rows);
            Console.WriteLine("PE1994 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, PE1994Rows);
            Console.WriteLine("PE1994 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, PE1994Rows);
            Console.WriteLine("PE1994 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, PE1994Rows);

            Console.WriteLine("PE1994"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => PE1994.IsElectoralEvent(_)),
                PE1994Rows,
                parties, municipalities, votingDistrict, wards);

            PE1994Rows.Clear();

            #endregion

            #region 1999 National & Provincial

            Console.WriteLine("NPE1999");

            List<NPE1999> NE1999Rows = [], PE1999Rows = [];

            foreach (NPE1999 row in UtilsCSVRows<NPE1999>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("1999 NPE.csv")).Open()))
                if (row.GetBallotType() == ElectoralEvent.Types.National)
                    NE1999Rows.Add(row);
                else if (row.GetBallotType() == ElectoralEvent.Types.Provincial)
                    PE1999Rows.Add(row);

            Console.WriteLine("NPE1999 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, NE1999Rows);
            Console.WriteLine("NPE1999 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, NE1999Rows);
            Console.WriteLine("NPE1999 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, NE1999Rows);
            Console.WriteLine("NPE1999 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, NE1999Rows);

            Console.WriteLine("NPE1999 - National"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE1999.IsElectoralEventNE(_)),
                NE1999Rows,
                parties, municipalities, votingDistrict, wards);

            Console.WriteLine("NPE1999 - Provincial"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE1999.IsElectoralEventPE(_)),
                PE1999Rows,
                parties, municipalities, votingDistrict, wards);

            NE1999Rows.Clear();
            PE1999Rows.Clear();

            #endregion

            #region 2000 Municipal

            Console.WriteLine("LGE2000");

            List<LGE2000> LGE2000Rows = UtilsCSVRows<LGE2000>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2000 LGE.csv")).Open()).ToList();

            Console.WriteLine("LGE2000 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, LGE2000Rows);
            Console.WriteLine("LGE2000 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, LGE2000Rows);
            Console.WriteLine("LGE2000 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, LGE2000Rows);
            Console.WriteLine("LGE2000 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, LGE2000Rows);

            Console.WriteLine("LGE2000 - Municipal"); CSV(
                sqliteConnection, logger, electoralevents.First(_ => LGE2000.IsElectoralEvent(_)),
                LGE2000Rows, parties, municipalities, votingDistrict, wards);

            LGE2000Rows.Clear();

            #endregion

            #region 2004 National & Provincial

            Console.WriteLine("NPE2004");

            List<NPE2004> NE2004Rows = [], PE2004Rows = [];

            foreach (NPE2004 row in UtilsCSVRows<NPE2004>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2004 NPE.csv")).Open()))
                if (row.GetBallotType() == ElectoralEvent.Types.National)
                    NE2004Rows.Add(row);
                else if (row.GetBallotType() == ElectoralEvent.Types.Provincial)
                    PE2004Rows.Add(row);

            Console.WriteLine("NPE2004 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, NE2004Rows);
            Console.WriteLine("NPE2004 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, NE2004Rows);
            Console.WriteLine("NPE2004 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, NE2004Rows);
            Console.WriteLine("NPE2004 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, NE2004Rows);

            Console.WriteLine("NPE2004 - National"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE2004.IsElectoralEventNE(_)),
                NE2004Rows,
                parties, municipalities, votingDistrict, wards);

            Console.WriteLine("NPE2004 - Provincial"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE2004.IsElectoralEventPE(_)),
                PE2004Rows,
                parties, municipalities, votingDistrict, wards);

            NE2004Rows.Clear();
            PE2004Rows.Clear();

            #endregion

            #region 2006 Municipal

            Console.WriteLine("LGE2006");

            List<LGE2006> LGE2006Rows = UtilsCSVRows<LGE2006>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2006 LGE.csv")).Open()).ToList();

            Console.WriteLine("LGE2006 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, LGE2006Rows);
            Console.WriteLine("LGE2006 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, LGE2006Rows);
            Console.WriteLine("LGE2006 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, LGE2006Rows);
            Console.WriteLine("LGE2006 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, LGE2006Rows);

            Console.WriteLine("LGE2006 - Municipal"); CSV(
                sqliteConnection, logger, electoralevents.First(_ => LGE2006.IsElectoralEvent(_)),
                LGE2006Rows, parties, municipalities, votingDistrict, wards);

            LGE2006Rows.Clear();

            #endregion

            #region 2009 National & Provincial

            Console.WriteLine("NPE2009");

            List<NPE2009> NE2009Rows = [], PE2009Rows = [];

            foreach (NPE2009 row in UtilsCSVRows<NPE2009>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2009 NPE.csv")).Open()))
                if (row.GetBallotType() == ElectoralEvent.Types.National)
                    NE2009Rows.Add(row);
                else if (row.GetBallotType() == ElectoralEvent.Types.Provincial)
                    PE2009Rows.Add(row);

            Console.WriteLine("NPE2009 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, NE2009Rows);
            Console.WriteLine("NPE2009 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, NE2009Rows);
            Console.WriteLine("NPE2009 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, NE2009Rows);
            Console.WriteLine("NPE2009 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, NE2009Rows);

            Console.WriteLine("NPE2009 - National"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE2009.IsElectoralEventNE(_)),
                NE2009Rows,
                parties, municipalities, votingDistrict, wards);

            Console.WriteLine("NPE2009 - Provincial"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE2009.IsElectoralEventPE(_)),
                PE2009Rows,
                parties, municipalities, votingDistrict, wards);

            NE2009Rows.Clear();
            PE2009Rows.Clear();

            #endregion

            #region 2011 Municipal

            Console.WriteLine("LGE2011");

            List<LGE2011> LGE2011Rows = UtilsCSVRows<LGE2011>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2011 LGE.csv")).Open()).ToList();

            Console.WriteLine("LGE2011 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, LGE2011Rows);
            Console.WriteLine("LGE2011 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, LGE2011Rows);
            Console.WriteLine("LGE2011 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, LGE2011Rows);
            Console.WriteLine("LGE2011 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, LGE2011Rows);

            Console.WriteLine("LGE2011 - Municipal"); CSV(
                sqliteConnection, logger, electoralevents.First(_ => LGE2011.IsElectoralEvent(_)),
                LGE2011Rows, parties, municipalities, votingDistrict, wards);

            LGE2011Rows.Clear();

            #endregion

            #region 2014 National & Provincial

            Console.WriteLine("NPE2014");

            List<NPE2014> NE2014Rows = [], PE2014Rows = [];

            foreach (NPE2014 row in UtilsCSVRows<NPE2014>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2014 NPE.csv")).Open()))
                if (row.GetBallotType() == ElectoralEvent.Types.National)
                    NE2014Rows.Add(row);
                else if (row.GetBallotType() == ElectoralEvent.Types.Provincial)
                    PE2014Rows.Add(row);

            Console.WriteLine("NPE2014 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, NE2014Rows);
            Console.WriteLine("NPE2014 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, NE2014Rows);
            Console.WriteLine("NPE2014 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, NE2014Rows);
            Console.WriteLine("NPE2014 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, NE2014Rows);

            Console.WriteLine("NPE2014 - National"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE2014.IsElectoralEventNE(_)),
                NE2014Rows,
                parties, municipalities, votingDistrict, wards);

            Console.WriteLine("NPE2014 - Provincial"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NPE2014.IsElectoralEventPE(_)),
                PE2014Rows,
                parties, municipalities, votingDistrict, wards);

            NE2014Rows.Clear();
            PE2014Rows.Clear();

            #endregion

            #region 2016 Municipal

            Console.WriteLine("LGE2016");

            List<LGE2016> LGE2016Rows = UtilsCSVRows<LGE2016>(
                logger,
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE EC.csv")).Open(),
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE FS.csv")).Open(),
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE GP.csv")).Open(),
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE LIM.csv")).Open(),
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE MP.csv")).Open(),
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE NC.csv")).Open(),
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE NW.csv")).Open(),
                datazip.Entries.First(entry => entry.FullName.EndsWith("2016 LGE WP.csv")).Open()).ToList();

            Console.WriteLine("LGE2016 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, LGE2016Rows);
            Console.WriteLine("LGE2016 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, LGE2016Rows);
            Console.WriteLine("LGE2016 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, LGE2016Rows);
            Console.WriteLine("LGE2016 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, LGE2016Rows);

            Console.WriteLine("LGE2016 - Municipal"); CSV(
                sqliteConnection, logger, electoralevents.First(_ => LGE2016.IsElectoralEvent(_)),
                LGE2016Rows, parties, municipalities, votingDistrict, wards);

            LGE2016Rows.Clear();

            #endregion

            #region 2019 National

            Console.WriteLine("NE2019");

            List<NE2019> NE2019Rows = UtilsCSVRows<NE2019>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2019 NE.csv")).Open())
                .Where(_ => _.Line is null || _.Line.Contains("Out of Country Voting") is false)
                .ToList();

            Console.WriteLine("NE2019 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, NE2019Rows);
            Console.WriteLine("NE2019 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, NE2019Rows);
            Console.WriteLine("NE2019 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, NE2019Rows);
            Console.WriteLine("NE2019 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, NE2019Rows);

            Console.WriteLine("NE2019"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => NE2019.IsElectoralEvent(_)),
                NE2019Rows,
                parties, municipalities, votingDistrict, wards);

            NE2019Rows.Clear();

            #endregion

            #region 2019 Provincial

            Console.WriteLine("PE2019");

            List<PE2019> PE2019Rows = UtilsCSVRows<PE2019>(logger, datazip.Entries.First(entry => entry.FullName.EndsWith("2019 PE.csv")).Open())
                .Where(_ => _.Line is null || _.Line.Contains("Out of Country Voting") is false)
                .ToList();

            Console.WriteLine("PE2019 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, PE2019Rows);
            Console.WriteLine("PE2019 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, PE2019Rows);
            Console.WriteLine("PE2019 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, PE2019Rows);
            Console.WriteLine("PE2019 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, PE2019Rows);

            Console.WriteLine("PE2019"); CSV(
                sqliteConnection, logger,
                electoralevents.First(_ => PE2019.IsElectoralEvent(_)),
                PE2019Rows,
                parties, municipalities, votingDistrict, wards);

            PE2019Rows.Clear();

            #endregion

            //#region 2021 Municipal

            //Console.WriteLine("LGE2021");

            //List<LGE2021> LGE2021Rows = UtilsCSVRows<LGE2021>(
            //    logger,
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE EC.csv")).Open(),
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE FS.csv")).Open(),
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE GP.csv")).Open(),
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE LIM.csv")).Open(),
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE MP.csv")).Open(),
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE NC.csv")).Open(),
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE NW.csv")).Open(),
            //    datazip.Entries.First(entry => entry.FullName.EndsWith("2021 LGE WP.csv")).Open())
            //.Where(_ => _.Line is null || _.Line.Contains("INDEPENDENT") is false)
            //.ToList();

            //Console.WriteLine("LGE2021 - New Parties"); parties = CSVNewParties(sqliteConnection, logger, LGE2021Rows);
            //Console.WriteLine("LGE2021 - New Municipalities"); municipalities = CSVNewMunicipalities(sqliteConnection, logger, LGE2021Rows);
            //Console.WriteLine("LGE2021 - New Voting Districts"); votingDistrict = CSVNewVotingDistricts(sqliteConnection, logger, LGE2021Rows);
            //Console.WriteLine("LGE2021 - New Wards"); wards = CSVNewWards(sqliteConnection, logger, LGE2021Rows);

            //Console.WriteLine("LGE2021 - Municipal"); CSV(
            //    sqliteConnection, logger, electoralevents.First(_ => LGE2021.IsElectoralEvent(_)),
            //    LGE2021Rows, parties, municipalities, votingDistrict, wards);

            //LGE2021Rows.Clear();

            //#endregion

            sqliteConnection.Close();

            if (File.Exists(dbzippath)) File.Delete(dbzippath);

            using FileStream dbfilestream = File.OpenRead(dbpath);
            using FileStream dbzipfilestream = File.Create(dbzippath);
            using ZipArchive dbziparchive = new(dbzipfilestream, ZipArchiveMode.Create, true);
            using Stream dbziparchivedbstream = dbziparchive.CreateEntry(dbname).Open();
            dbfilestream.CopyTo(dbziparchivedbstream);
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
                    (ballot.type is null && csvrowlge.BallotType is not null) ||
                    (ballot.type is not null && csvrowlge.BallotType is null) ||
                    (string.Equals(ballot.type, csvrowlge.BallotType) is false);

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
                (ballot.pkProvince is null && row.ProvincePk is not null) ||
                (ballot.pkProvince is not null && row.ProvincePk is null) ||
                (ballot.pkProvince != row.ProvincePk);

            return change;
        }
        public static void CSV<TCSVRow>(
            SQLiteConnection sqliteConnection,
            StreamWriter log,
            ElectoralEvent electoralEvent,
            IEnumerable<TCSVRow> rows,
            List<Party>? parties,
            List<Municipality>? municipalities,
            List<VotingDistrict>? votingdistricts,
            List<Ward>? wards) where TCSVRow : CSVRow
        {
            string[] electoralEventBallotsKeys = [string.Empty];
            Dictionary<int, int> electoralEventBallotPkPartyVotes = [];
            Dictionary<string, Ballot> electoralEventBallots = new()
            {
                { electoralEventBallotsKeys[0], new Ballot
                {
                    pkElectoralEvent = electoralEvent.pk,
                    type = electoralEvent.type,
                } }
            };
            electoralEventBallots[string.Empty] = sqliteConnection.CreateAndAdd(electoralEventBallots[string.Empty]);
            electoralEvent.list_pkBallot = Utils.CSV.AddPKIfUnique(electoralEvent.list_pkBallot, electoralEventBallots[string.Empty].pk);

            List<Ballot> ballots = [];
            parties ??= [];
            votingdistricts ??= [];
            wards ??= [];
            municipalities ??= [];

            Ballot? currentBallot = null;
            VotingDistrict? currentVotingDistrict = null;
            Party? currentParty = null;
            Ward? currentWard = null;
            Municipality? currentMunicipality = null;

            IOrderedEnumerable<CSVRow> rowsordered = rows.OrderBy(row => row.ProvincePk);
            rowsordered = typeof(TCSVRow) == typeof(NPE1999)
                ? rowsordered.ThenBy(row => row.MunicipalityName)
                : rowsordered.ThenBy(row => row.MunicipalityGeo);
            rowsordered = rowsordered.ThenBy(row => row.GetWardId());
            rowsordered = rowsordered.ThenBy(row => row.VotingDistrictId);
            rowsordered = rowsordered.ThenBy(row => row.GetBallotType());

            IEnumerator<CSVRow> rowsorderedenumerator = rowsordered.GetEnumerator();

            for (int index = 0; rowsorderedenumerator.MoveNext(); index++)
            {
                Console.WriteLine("Row {0} / {1}", index, rows.Count());

                if (currentBallot is null || ChangeBallot(
                    rowsorderedenumerator.Current,
                    currentBallot,
                    currentVotingDistrict?.id,
                    currentWard?.id,
                    typeof(TCSVRow) == typeof(NPE1999) ? currentMunicipality?.name : currentMunicipality?.geoCode))
                {
                    if (currentBallot?.pkElectoralEvent is not null)
                        ballots.Add(currentBallot);

                    if (rowsorderedenumerator.Current.GetWardId() is not string wardid)
                        currentWard = null;
                    else if (currentWard is null || currentWard.id != wardid)
                    {
                        if (wards.Find(ward => ward.id == wardid) is Ward ward)
                            currentWard = ward;
                        else if (sqliteConnection.Table<Ward>().FirstOrDefault(_ => _.id == wardid) is Ward wardsql)
                        {
                            currentWard = wardsql;
                            wards.Add(currentWard);
                        }
                    }

                    if (rowsorderedenumerator.Current.VotingDistrictId is null)
                        currentVotingDistrict = null;
                    else if (currentVotingDistrict is null || currentVotingDistrict.id != rowsorderedenumerator.Current.VotingDistrictId)
                    {
                        if (votingdistricts.Find(votingdistrict => votingdistrict.id == rowsorderedenumerator.Current.VotingDistrictId) is VotingDistrict votingdistrict)
                            currentVotingDistrict = votingdistrict;
                        else if (sqliteConnection.Table<VotingDistrict>().FirstOrDefault(_ => _.id == rowsorderedenumerator.Current.VotingDistrictId) is VotingDistrict votingdistrictsql)
                        {
                            currentVotingDistrict = votingdistrictsql;
                            votingdistricts.Add(currentVotingDistrict);
                        }
                    }

                    if (typeof(TCSVRow) == typeof(NPE1999))
                    {
                        if (rowsorderedenumerator.Current.MunicipalityName is null)
                            currentMunicipality = null;
                        else if (currentMunicipality is null || currentMunicipality.name != rowsorderedenumerator.Current.MunicipalityName)
                        {
                            if (municipalities.Find(municipality => municipality.name == rowsorderedenumerator.Current.MunicipalityName) is Municipality municipality)
                                currentMunicipality = municipality;
                            else if (sqliteConnection.Table<Municipality>().FirstOrDefault(_ => _.name == rowsorderedenumerator.Current.MunicipalityName) is Municipality municipalitysql)
                            {
                                currentMunicipality = municipalitysql;
                                municipalities.Add(municipalitysql);
                            }
                        }
                    }
                    else
                    {
                        if (rowsorderedenumerator.Current.MunicipalityGeo is null)
                            currentMunicipality = null;
                        else if (currentMunicipality is null || currentMunicipality.geoCode != rowsorderedenumerator.Current.MunicipalityGeo)
                        {
                            if (municipalities.Find(municipality => municipality.geoCode == rowsorderedenumerator.Current.MunicipalityGeo) is Municipality municipality)
                                currentMunicipality = municipality;
                            else if (sqliteConnection.Table<Municipality>().FirstOrDefault(_ => _.geoCode == rowsorderedenumerator.Current.MunicipalityGeo) is Municipality municipalitysql)
                            {
                                currentMunicipality = municipalitysql;
                                municipalities.Add(municipalitysql);
                            }
                        }
                    }

                    if (currentVotingDistrict is not null)
                    {
                        currentVotingDistrict.pkWard ??= currentWard?.pk;
                        currentVotingDistrict.pkMunicipality ??= currentMunicipality?.pk;
                        currentVotingDistrict.pkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                    }
                    if (currentWard is not null)
                    {
                        currentWard.pkMunicipality ??= currentMunicipality?.pk;
                        currentWard.pkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                        currentWard.list_pkVotingDistrict = Utils.CSV.AddPKIfUnique(currentWard.list_pkVotingDistrict, currentVotingDistrict?.pk);
                    }
                    if (currentMunicipality is not null)
                    {
                        currentMunicipality.pkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                        currentMunicipality.list_pkWard = Utils.CSV.AddPKIfUnique(currentMunicipality.list_pkWard, currentWard?.pk);
                    }           

                    currentBallot = rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.pkElectoralEvent = typeof(TCSVRow) == typeof(NE1994) || typeof(TCSVRow) == typeof(PE1994) ? null : electoralEvent.pk;
                        ballot.pkProvince = rowsorderedenumerator.Current.ProvincePk;
                        ballot.pkMunicipality = currentMunicipality?.pk;
                        ballot.pkWard = currentWard?.pk;
                        ballot.pkVotingDistrict = currentVotingDistrict?.pk;
                    });

                    switch (true)
                    {
                        case true when currentBallot.type is null:
                        case true when currentBallot.pkProvince is null:
                            break;

                        case true when (currentBallot.type == ElectoralEvent.Types.National || currentBallot.type == ElectoralEvent.Types.Provincial) &&
                            sqliteConnection.Find<Province>(currentBallot.pkProvince.Value) is Province province &&
                            string.Format("{0}.{1}", currentBallot.type, province.id) is string type:
                            {
                                electoralEventBallotsKeys = [string.Empty, type];
                                if (electoralEventBallots.ContainsKey(type) is false)
                                {
                                    electoralEventBallots.Add(type, rowsorderedenumerator.Current.AsBallot(ballot => ballot.type = type));
                                    electoralEventBallots[type].UpdateBallot(currentBallot);
                                    electoralEventBallots[type] = sqliteConnection.CreateAndAdd(electoralEventBallots[type]);
                                    electoralEvent.list_pkBallot = Utils.CSV.AddPKIfUnique(electoralEvent.list_pkBallot, electoralEventBallots[type].pk);
                                }

                            }
                            break;

                        case true when
                            currentBallot.type.Split('.') is string[] typesplit &&
                            typesplit.Length == 2 &&
                            typesplit[0] == ElectoralEvent.Types.Municipal &&
                            electoralEventBallots.ContainsKey(currentBallot.type) is false:
                            {
                                electoralEventBallotsKeys = [string.Empty, currentBallot.type];
                                electoralEventBallots.Add(currentBallot.type, rowsorderedenumerator.Current.AsBallot(ballot => ballot.pkProvince = null));
                                electoralEventBallots[currentBallot.type].UpdateBallot(currentBallot);
                                electoralEventBallots[currentBallot.type] = sqliteConnection.CreateAndAdd(electoralEventBallots[currentBallot.type]);
                                electoralEvent.list_pkBallot = Utils.CSV.AddPKIfUnique(electoralEvent.list_pkBallot, electoralEventBallots[currentBallot.type].pk);

                            }
                            break;

                        default: break;
                    }

                    electoralEvent.list_pkBallot = Utils.CSV.AddPKIfUnique(electoralEvent.list_pkBallot, electoralEventBallots[string.Empty].pk);
                    foreach (string electoralEventBallotsKey in electoralEventBallotsKeys)
                        if (electoralEventBallots.ContainsKey(electoralEventBallotsKey))
                            electoralEventBallots[electoralEventBallotsKey].UpdateBallot(currentBallot);
                }

                if (rowsorderedenumerator.Current.PartyName is null)
                    currentParty = null;
                else if (currentParty is null || currentParty.name != rowsorderedenumerator.Current.PartyName)
                {
                    if (parties.Find(party => party.name == rowsorderedenumerator.Current.PartyName) is Party party)
                        currentParty = party;
                    else if (sqliteConnection.Table<Party>().FirstOrDefault(_ => _.name == rowsorderedenumerator.Current.PartyName) is Party partysql)
                    {
                        currentParty = partysql;
                        parties.Add(partysql);
                    }
                }

                if (currentParty is not null && rowsorderedenumerator.Current.PartyVotes.HasValue)
                {
                    if (electoralEventBallotPkPartyVotes.ContainsKey(currentParty.pk))
                        electoralEventBallotPkPartyVotes[currentParty.pk] += rowsorderedenumerator.Current.PartyVotes.Value;
                    else electoralEventBallotPkPartyVotes.Add(currentParty.pk, rowsorderedenumerator.Current.PartyVotes.Value);

                    currentBallot.list_pkParty_votes = 
                            Utils.CSV.AddPKPairIfUnique(currentBallot.list_pkParty_votes, currentParty.pk, rowsorderedenumerator.Current.PartyVotes.Value);

                    foreach (string electoralEventBallotsKey in electoralEventBallotsKeys)
                        if (electoralEventBallots.ContainsKey(electoralEventBallotsKey))
                            electoralEventBallots[electoralEventBallotsKey].list_pkParty_votes = Utils.CSV.AddPKPairIfUnique(
                                electoralEventBallots[electoralEventBallotsKey].list_pkParty_votes,
                                currentParty.pk,
                                rowsorderedenumerator.Current.PartyVotes.Value,
                                true);
                }
            }

            if (currentBallot?.pkElectoralEvent is not null)
                ballots.Add(currentBallot);

            Console.WriteLine("Inserting Ballots");
            int ballotsAdded = sqliteConnection.InsertAll(ballots);
            Console.WriteLine("Updating Municipalities");
            sqliteConnection.UpdateAll(municipalities);
            Console.WriteLine("Updating VotingDistricts");
            sqliteConnection.UpdateAll(votingdistricts);
            Console.WriteLine("Updating Wards");
            sqliteConnection.UpdateAll(wards);
            Console.WriteLine("Updating Parties");
            foreach (Party party in parties)
                party.list_pkElectoralEvent = Utils.CSV.AddPKIfUnique(party.list_pkElectoralEvent, electoralEvent.pk);
            sqliteConnection.UpdateAll(parties);
            Console.WriteLine("Updating ElectoralEvent");
            sqliteConnection.Update(electoralEvent);
            foreach (Ballot electoralEventBallot in electoralEventBallots.Values)
                sqliteConnection.Update(electoralEventBallot);
        }
    }
}
