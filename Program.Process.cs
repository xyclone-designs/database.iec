using Database.IEC.Inputs;

using SQLite;

using System;
using System.Collections.Generic;
using System.Linq;

using XycloneDesigns.Apis.General.Tables;
using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC
{
    internal partial class Program
    {
        static Ballot? ElectoralEventBallot = null;
        static Ballot? CurrentBallot = null;
        static VotingDistrict? CurrentVotingDistrict = null;
        static Province? CurrentProvince = null;
        static Party? CurrentParty = null;
        static Ward? CurrentWard = null;
        static Municipality? CurrentMunicipality = null;

        public static void Process<TCSVRow>(Parameters<TCSVRow> parameters) where TCSVRow : CSV.Row
		{
			Console.WriteLine(parameters.LoggerTag);

			IEnumerable<TCSVRow> rows = Enumerable.Empty<TCSVRow>()
				.Concat(parameters.Rows1 ?? Enumerable.Empty<TCSVRow>())
				.Concat(parameters.Rows2 ?? Enumerable.Empty<TCSVRow>());

			if (parameters.New is false || (parameters.NewMunicipalities ?? false))
			{
				Console.WriteLine("{0} - New Municipalities", parameters.LoggerTag); parameters.Municipalities =
					parameters.SqliteConnectionMunicipalities?.CSVNewMunicipalities(parameters.Logger, rows);
			}
			if (parameters.New is false || (parameters.NewParties ?? false))
			{
				Console.WriteLine("{0} - New Parties", parameters.LoggerTag); parameters.Parties =
					parameters.SqliteConnection?.CSVNewParties(parameters.Logger, rows);
			}
			if (parameters.New is false || (parameters.NewVotingDistricts ?? false))
			{
				Console.WriteLine("{0} - New VotingDistricts", parameters.LoggerTag); parameters.VotingDistricts =
					parameters.SqliteConnection?.CSVNewVotingDistricts(parameters.Logger, rows);
			}
			if (parameters.New is false || (parameters.NewWards ?? false))
			{
				Console.WriteLine("{0} - New Wards", parameters.LoggerTag); parameters.Wards =
					parameters.SqliteConnection?.CSVNewWards(parameters.Logger, rows);
			}

			if (parameters.New) return;

			Console.WriteLine("{0} - CSVs.CSV", parameters.LoggerTag);

			if (parameters.ElectoralEvent1 is not null && parameters.Rows1 is not null)
			{
                ProcessCSV(new Parameters<TCSVRow>(parameters) { ElectoralEvent1 = parameters.ElectoralEvent1, Rows1 = parameters.Rows1, });
				
				parameters.Rows1.Clear();				
			}
			if (parameters.ElectoralEvent2 is not null && parameters.Rows2 is not null)
			{
                ProcessCSV(new Parameters<TCSVRow>(parameters) { ElectoralEvent1 = parameters.ElectoralEvent2, Rows1 = parameters.Rows2, });

				parameters.Rows2.Clear();
			}

			parameters.ExtraAction?.Invoke(parameters);

			if (parameters.ElectoralEvent1 is not null)
			{
				string pathdb = ElectoralEventPath(DirectoryOutput, parameters.ElectoralEvent1, "db");
				string pathtxt = ElectoralEventPath(DirectoryOutput, parameters.ElectoralEvent1, "txt");

				PksToText(pathtxt, parameters);

				if (parameters.SqliteConnection?
					.Table<Ballot>()
					.Where(ballot => ballot.PkElectoralEvent == parameters.ElectoralEvent1.Pk) is TableQuery<Ballot> ballots)
				{
					using SQLiteConnection connection = SQLiteConnection(pathdb);

					connection.InsertAll(ballots);
					connection.Close();
				}
			}
			if (parameters.ElectoralEvent2 is not null)
			{
				string pathdb = ElectoralEventPath(DirectoryOutput, parameters.ElectoralEvent2, "db");
				string pathtxt = ElectoralEventPath(DirectoryOutput, parameters.ElectoralEvent2, "txt");

				PksToText(pathtxt, parameters);

				if (parameters.SqliteConnection?
					.Table<Ballot>()
					.Where(ballot => ballot.PkElectoralEvent == parameters.ElectoralEvent2.Pk) is TableQuery<Ballot> ballots)
				{
					using SQLiteConnection connection = SQLiteConnection(pathdb);

					connection.InsertAll(ballots);
					connection.Close();
				}
			}
		}
        public static void ProcessCSV<TCSVRow>(Parameters<TCSVRow> parameters) where TCSVRow : CSV.Row
        {
            parameters.Rows1 ??= [];
            parameters.Ballots = [];
            parameters.BallotsElectoralEvent = [];

            IOrderedEnumerable<CSV.Row> rowsordered = parameters.Rows1.OrderBy(row => row.ProvincePk);
            rowsordered = typeof(TCSVRow) == typeof(CSV.NPE._1999)
                ? rowsordered.ThenBy(row => row.MunicipalityName)
                : rowsordered.ThenBy(row => row.MunicipalityGeo);
            rowsordered = rowsordered.ThenBy(row => row.GetWardId());
            rowsordered = rowsordered.ThenBy(row => row.VotingDistrictId);
            rowsordered = rowsordered.ThenBy(row => row.GetBallotType());

            IEnumerator<CSV.Row> rowsorderedenumerator = rowsordered.GetEnumerator();

            for (int index = 0; rowsorderedenumerator.MoveNext(); index++)
            {
                Console.WriteLine("Row {0} / {1}", index, parameters.Rows1.Count);

                if (CurrentBallot is null || ChangeBallot(
                    rowsorderedenumerator.Current,
                    CurrentBallot,
                    CurrentVotingDistrict?.Id,
                    CurrentWard?.Id,
                    typeof(TCSVRow) == typeof(CSV.NPE._1999) ? CurrentMunicipality?.Name : CurrentMunicipality?.GeoCode))
                {
                    if (CurrentBallot?.PkElectoralEvent is not null)
                    {
                        parameters.Ballots.Add(CurrentBallot);
                        ElectoralEventBallot?.UpdateBallot(CurrentBallot);
                    }

                    ProcessCSVRowWard(rowsorderedenumerator.Current, parameters);
                    ProcessCSVRowProvince(rowsorderedenumerator.Current, parameters);
                    ProcessCSVRowVotingDistrict(rowsorderedenumerator.Current, parameters);
                    ProcessCSVRowMunicipality(rowsorderedenumerator.Current, parameters);

                    CurrentVotingDistrict?.PkWard ??= CurrentWard?.Pk;
                    CurrentVotingDistrict?.PkMunicipality ??= CurrentMunicipality?.Pk;
                    CurrentVotingDistrict?.PkProvince ??= rowsorderedenumerator.Current.ProvincePk;

                    CurrentWard?.PkMunicipality ??= CurrentMunicipality?.Pk;
                    CurrentWard?.PkProvince ??= rowsorderedenumerator.Current.ProvincePk;

                    CurrentMunicipality?.PkProvince ??= rowsorderedenumerator.Current.ProvincePk;

                    CurrentBallot = rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.PkElectoralEvent ??= parameters.ElectoralEvent1?.Pk;
                        ballot.PkProvince ??= CurrentProvince?.Pk ?? rowsorderedenumerator.Current.ProvincePk;
                        ballot.PkMunicipality ??= CurrentMunicipality?.Pk;
                        ballot.PkWard ??= CurrentWard?.Pk;
                        ballot.PkVotingDistrict ??= CurrentVotingDistrict?.Pk;
                    });

                    ElectoralEventBallot ??= rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.PkElectoralEvent = parameters.ElectoralEvent1?.Pk;
                        ballot.PkProvince = null;
                        ballot.PkMunicipality = null;
                        ballot.PkWard = null;
                        ballot.PkVotingDistrict = null;

                        if (parameters.ElectoralEvent1?.Type?.Contains(ElectoralEvent.Types.Municipal) ?? false)
                            ballot.Type = ElectoralEvent.Types.Municipal;
                    });
                }

                ProcessCSVRowParty(rowsorderedenumerator.Current, parameters);

                if (CurrentParty is not null && rowsorderedenumerator.Current.PartyVotes.HasValue)
                    CurrentBallot.List_PkParty_Votes = GeneralTable.AddPKPairIfUnique(
                        CurrentBallot.List_PkParty_Votes,
                        CurrentParty.Pk,
                        rowsorderedenumerator.Current.PartyVotes.Value);
            }

            if (CurrentBallot?.PkElectoralEvent is not null)
                parameters.Ballots.Add(CurrentBallot);

            Console.WriteLine("Inserting Event Ballots");

            if (ElectoralEventBallot is not null)
            {
                if (ElectoralEvent.IsMunicipal(parameters.ElectoralEvent1?.Type))
                    ElectoralEventBallot.VotersRegistered = null;

                if (parameters.SqliteConnection is not null)
                {
                    ElectoralEventBallot = parameters.SqliteConnection.InsertAndReturn(ElectoralEventBallot);

                    parameters.BallotsElectoralEvent.Add(ElectoralEventBallot);
                    parameters.ElectoralEvent1?.List_PkBallot = GeneralTable.AddPKIfUnique(parameters.ElectoralEvent1.List_PkBallot, ElectoralEventBallot.Pk);
                }
            }

            foreach (Ballot eventballot in parameters.Ballots
                .Where(_ballot => string.IsNullOrWhiteSpace(_ballot.Type) is false && _ballot.PkProvince.HasValue)
                .GroupBy(_ballot =>
                {
                    return
                        _ballot.Type!.Split('.') is string[] typesplit &&
                        typesplit.Length == 2 &&
                        typesplit[0] == ElectoralEvent.Types.Municipal
                            ? _ballot.Type
                            : string.Format("{0}.{1}", _ballot.Type, CSV.Utils.ProvincePkToId(_ballot.PkProvince!.Value));
                })
                .Where(groupedballot => groupedballot.Any())
                .SelectMany(groupedballot =>
                {
                    IEnumerable<Ballot> ballots = [];
                    bool ismunicipal = groupedballot.Key.Contains(ElectoralEvent.Types.Municipal, StringComparison.OrdinalIgnoreCase);
                    Ballot _eventballot = new() { Type = groupedballot.Key, };
                    Ballot? _municipalityballot = null;

                    foreach (Ballot ballot in groupedballot.OrderBy(_ => _.PkProvince).ThenBy(_ => _.PkMunicipality))
                    {
                        if (ismunicipal)
                        {
                            if (_municipalityballot is null ||
                                (ballot.PkMunicipality is not null && _municipalityballot.PkMunicipality is null) ||
                                (ballot.PkMunicipality is null && _municipalityballot.PkMunicipality is not null) ||
                                (
                                    ballot.PkMunicipality is not null &&
                                    _municipalityballot.PkMunicipality is not null &&
                                    _municipalityballot.PkMunicipality.Value != ballot.PkMunicipality.Value

                                ))
                            {
                                if (_municipalityballot is not null)
                                    ballots = ballots.Append(_municipalityballot);

                                _municipalityballot = new() { Type = groupedballot.Key, };
                            }

                            _municipalityballot.PkProvince ??= ballot.PkProvince;
                            _municipalityballot.PkMunicipality ??= ballot.PkMunicipality;
                            _municipalityballot.PkElectoralEvent ??= ballot.PkElectoralEvent;
                            _municipalityballot.UpdateBallot(ballot);
                        }

                        _eventballot.UpdateBallot(ballot);
                        _eventballot.PkProvince ??= ismunicipal ? null : ballot.PkProvince;
                        _eventballot.PkElectoralEvent ??= ballot.PkElectoralEvent;
                    }

                    ElectoralEventBallot?.UpdatePartyVotes(_eventballot.List_PkParty_Votes);

                    return ballots.Prepend(_eventballot);

                }).OrderBy(_ => _.PkMunicipality.HasValue))
            {
                Ballot? added = parameters.SqliteConnection?.InsertAndReturn(eventballot);

                if (added is not null)
                    parameters.BallotsElectoralEvent.Add(added);

                if (parameters.ElectoralEvent1 is not null)
                {
                    if (ElectoralEvent.IsMunicipal(parameters.ElectoralEvent1.Type))
                    {
                        ElectoralEventBallot?.VotersRegistered = Math.Max(eventballot.VotersRegistered ?? 0, ElectoralEventBallot.VotersRegistered ?? 0);

                        if (added?.PkMunicipality is null)
                            parameters.ElectoralEvent1.List_PkBallot = GeneralTable.AddPKIfUnique(parameters.ElectoralEvent1.List_PkBallot, added?.Pk);
                    }
                    else parameters.ElectoralEvent1.List_PkBallot = GeneralTable.AddPKIfUnique(parameters.ElectoralEvent1.List_PkBallot, added?.Pk);
                }
            }

            parameters.SqliteConnection?.Update(ElectoralEventBallot);

            if (parameters.SqliteConnection is not null)
            {
                Console.WriteLine("Updating ElectoralEvent"); parameters.SqliteConnection.Update(parameters.ElectoralEvent1);
                Console.WriteLine("Updating VotingDistricts"); parameters.SqliteConnection.UpdateAll(parameters.VotingDistricts);
                Console.WriteLine("Updating Wards"); parameters.SqliteConnection.UpdateAll(parameters.Wards);

                Console.WriteLine("Inserting Ballots");
                if (parameters.ElectoralEvent1?.Pk != 1 && parameters.ElectoralEvent1?.Pk != 2)
                    parameters.SqliteConnection.InsertAll(parameters.Ballots);

                Console.WriteLine("Updating Parties");
                if (parameters.Parties is not null)
                {
                    foreach (Party party in parameters.Parties)
                        party.List_PkElectoralEvent = GeneralTable.AddPKIfUnique(party.List_PkElectoralEvent, parameters.ElectoralEvent1?.Pk);
                    parameters.SqliteConnection.UpdateAll(parameters.Parties);
                }
            }

            if (parameters.SqliteConnectionProvinces is not null)
            {
                Console.WriteLine("Updating Provinces"); parameters.SqliteConnectionProvinces.UpdateAll(parameters.Provinces);
            }

            if (parameters.SqliteConnectionMunicipalities is not null)
            {
                Console.WriteLine("Updating Municipalities"); parameters.SqliteConnectionMunicipalities.UpdateAll(parameters.Municipalities);
            }

            if (parameters.ElectoralEvent1 is not null)
                parameters.ElectoralEvents?.Add(parameters.ElectoralEvent1);
        }
        public static void ProcessCSVRowMunicipality<TCSVRow>(CSV.Row row, Parameters<TCSVRow> parameters) where TCSVRow : CSV.Row
        {
            if (typeof(TCSVRow) == typeof(CSV.NPE._1999))
            {
                if (row.MunicipalityName is null)
                    CurrentMunicipality = null;
                else if (CurrentMunicipality is null || string.Equals(CurrentMunicipality.Name, row.MunicipalityName, StringComparison.OrdinalIgnoreCase) is false)
                {
                    parameters.Municipalities ??= [];

                    if (parameters.Municipalities.Find(_ => string.Equals(_.Name, row.MunicipalityName, StringComparison.OrdinalIgnoreCase)) is Municipality municipality)
                        CurrentMunicipality = municipality;
                    else if (parameters.SqliteConnectionMunicipalities?.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => _.Name == row.MunicipalityName) is Municipality municipalitysql)
                    {
                        CurrentMunicipality = municipalitysql;
                        parameters.Municipalities.Add(municipalitysql);
                    }
                }
            }
            else
            {
                if (row.MunicipalityGeo is null)
                    CurrentMunicipality = null;
                else if (CurrentMunicipality is null || string.Equals(CurrentMunicipality.GeoCode, row.MunicipalityGeo, StringComparison.OrdinalIgnoreCase) is false)
                {
                    parameters.Municipalities ??= [];

                    if (parameters.Municipalities.Find(_ => string.Equals(_.GeoCode, row.MunicipalityGeo, StringComparison.OrdinalIgnoreCase)) is Municipality municipality)
                        CurrentMunicipality = municipality;
                    else if (parameters.SqliteConnectionMunicipalities?.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.GeoCode, row.MunicipalityGeo, StringComparison.OrdinalIgnoreCase)) is Municipality municipalitysql)
                    {
                        CurrentMunicipality = municipalitysql;
                        parameters.Municipalities.Add(municipalitysql);
                    }
                }
            }
        }
        public static void ProcessCSVRowProvince<TCSVRow>(CSV.Row row, Parameters<TCSVRow> parameters) where TCSVRow : CSV.Row
        {
            if (row.ProvincePk is null)
                CurrentProvince = null;
            else if (CurrentProvince is null || CurrentProvince.Pk != row.ProvincePk)
            {
                parameters.Provinces ??= [];

                if (parameters.Provinces.Find(currentProvince => currentProvince.Pk == row.ProvincePk) is Province Province)
                    CurrentProvince = Province;
                else if (parameters.SqliteConnectionProvinces?.Table<Province>().FirstOrDefault(_ => _.Pk == row.ProvincePk) is Province Provincesql)
                {
                    CurrentProvince = Provincesql;
                    parameters.Provinces.Add(CurrentProvince);
                }
            }
        }
        public static void ProcessCSVRowParty<TCSVRow>(CSV.Row row, Parameters<TCSVRow> parameters) where TCSVRow : CSV.Row
        {
            if (row.PartyName is null)
                CurrentParty = null;
            else if (CurrentParty is null || string.Equals(CurrentParty.Name, row.PartyName, StringComparison.OrdinalIgnoreCase) is false)
            {
                parameters.Parties ??= [];

                if (parameters.Parties.Find(party => string.Equals(party.Name, row.PartyName, StringComparison.OrdinalIgnoreCase)) is Party party)
                    CurrentParty = party;
                else if (parameters.SqliteConnection?.Table<Party>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.Name, row.PartyName, StringComparison.OrdinalIgnoreCase)) is Party partysql)
                {
                    CurrentParty = partysql;
                    parameters.Parties.Add(partysql);
                }
            }
        }
        public static void ProcessCSVRowVotingDistrict<TCSVRow>(CSV.Row row, Parameters<TCSVRow> parameters) where TCSVRow : CSV.Row
        {
            if (row.VotingDistrictId is null)
                CurrentVotingDistrict = null;
            else if (CurrentVotingDistrict is null || CurrentVotingDistrict.Id != row.VotingDistrictId)
            {
                parameters.VotingDistricts ??= [];

                if (parameters.VotingDistricts.Find(votingdistrict => votingdistrict.Id == row.VotingDistrictId) is VotingDistrict votingdistrict)
                    CurrentVotingDistrict = votingdistrict;
                else if (parameters.SqliteConnection?.Table<VotingDistrict>().FirstOrDefault(_ => _.Id == row.VotingDistrictId) is VotingDistrict votingdistrictsql)
                {
                    CurrentVotingDistrict = votingdistrictsql;
                    parameters.VotingDistricts.Add(CurrentVotingDistrict);
                }
            }
        }
        public static void ProcessCSVRowWard<TCSVRow>(CSV.Row row, Parameters<TCSVRow> parameters) where TCSVRow : CSV.Row
        {
            if (row.GetWardId() is not string wardid)
                CurrentWard = null;
            else if (CurrentWard is null || CurrentWard.Id != wardid)
            {
                parameters.Wards ??= [];

                if (parameters.Wards.Find(ward => ward.Id == wardid) is Ward ward)
                    CurrentWard = ward;
                else if (parameters.SqliteConnection?.Table<Ward>().FirstOrDefault(_ => _.Id == wardid) is Ward wardsql)
                {
                    CurrentWard = wardsql;
                    parameters.Wards.Add(CurrentWard);
                }
            }
        }
        public static void ProcessXLS<TXLSSeats, TXLSSeatsRow>(SQLiteConnection sqliteConnection, ElectoralEvent electoralEvent, IEnumerable<TXLSSeats> seats)
            where TXLSSeatsRow : XLS.Row
            where TXLSSeats : XLS.Seats<TXLSSeatsRow>
        {
            Ballot ballotmunicipalitypr = sqliteConnection.Table<Ballot>()
                .First(ballot =>
                    ballot.Type != null &&
                    ballot.Type.Contains("pr") &&
                    ballot.PkElectoralEvent == electoralEvent.Pk &&
                    ballot.PkProvince == null &&
                    ballot.PkMunicipality == null &&
                    ballot.PkVotingDistrict == null &&
                    ballot.PkWard == null);
            Ballot ballotmunicipalityward = sqliteConnection.Table<Ballot>()
                .First(ballot =>
                    ballot.Type != null &&
                    ballot.Type.Contains("ward") &&
                    ballot.PkElectoralEvent == electoralEvent.Pk &&
                    ballot.PkProvince == null &&
                    ballot.PkMunicipality == null &&
                    ballot.PkVotingDistrict == null &&
                    ballot.PkWard == null);
            List<Ballot> ballotsmunicipality = sqliteConnection.Table<Ballot>()
                .Where(ballot =>
                    ballot.Type != null &&
                    (ballot.Type.Contains("pr") || ballot.Type.Contains("ward")) &&
                    ballot.PkElectoralEvent == electoralEvent.Pk &&
                    ballot.PkProvince != null &&
                    ballot.PkMunicipality != null &&
                    ballot.PkVotingDistrict == null &&
                    ballot.PkWard == null)
                .ToList();

            ballotmunicipalitypr.List_PkParty_Seats = null;
            ballotmunicipalityward.List_PkParty_Seats = null;
            foreach (Ballot ballotmunicipality in ballotsmunicipality)
                ballotmunicipality.List_PkParty_Seats = null;

            foreach (Ballot ballotmunicipality in ballotsmunicipality)
            {
                Municipality municipality = sqliteConnection.Find<Municipality>(ballotmunicipality.PkMunicipality);
                TXLSSeats? txlsseats = seats.FirstOrDefault(_seat => string.Equals(_seat.MunicipalityGeo, municipality?.GeoCode, StringComparison.OrdinalIgnoreCase));

                if (txlsseats?.Rows != null)
                    foreach (TXLSSeatsRow txlsseatsrow in txlsseats.Rows)
                    {
                        bool
                            isPr = ballotmunicipality.Type!.Contains("pr"),
                            isWard = ballotmunicipality.Type!.Contains("ward");

                        (int? PartySeats, string? PartyName) = true switch
                        {
                            true when txlsseatsrow is XLS.RowLGE1 xlsrowlge1 && isPr => ((int?)xlsrowlge1.PRListSeats, xlsrowlge1.PartyName),
                            true when txlsseatsrow is XLS.RowLGE1 xlsrowlge1 && isWard => ((int?)xlsrowlge1.WardSeats, xlsrowlge1.PartyName),
                            true when txlsseatsrow is XLS.RowLGE2 xlsrowlge2 => ((int?)xlsrowlge2.TotalPartySeats, xlsrowlge2.PartyName),
                            true when txlsseatsrow is XLS.RowLGE3 xlsrowlge3 => ((int?)xlsrowlge3.TotalPartySeats, xlsrowlge3.PartyName),

                            _ => (new int?(), null)
                        };

                        if (PartySeats.HasValue)
                        {
                            Party party = sqliteConnection.Table<Party>().AsEnumerable().First(_party => string.Equals(_party.Name, PartyName, StringComparison.OrdinalIgnoreCase));

                            ballotmunicipality.List_PkParty_Seats = GeneralTable.AddPKPairIfUnique(ballotmunicipality.List_PkParty_Seats, party.Pk, PartySeats.Value, true);
                            if (ballotmunicipalitypr is not null && ballotmunicipality.Type!.Contains("pr"))
                                ballotmunicipalitypr.List_PkParty_Seats = GeneralTable.AddPKPairIfUnique(ballotmunicipalitypr.List_PkParty_Seats, party.Pk, PartySeats.Value, true);
                            if (ballotmunicipalityward is not null && ballotmunicipality.Type!.Contains("ward"))
                                ballotmunicipalityward.List_PkParty_Seats = GeneralTable.AddPKPairIfUnique(ballotmunicipalityward.List_PkParty_Seats, party.Pk, PartySeats.Value, true);
                        }
                    }
            }

            sqliteConnection.Update(ballotmunicipalitypr, typeof(Ballot));
            sqliteConnection.Update(ballotmunicipalityward, typeof(Ballot));
            sqliteConnection.UpdateAll(ballotsmunicipality);
        }

        public static bool ChangeBallot<TCSVRow>(TCSVRow row, Ballot? ballot, string? votingdistrictid, string? wardid, string? municipalityvalue) where TCSVRow : CSV.Row
        {
            if (ballot is null) return true;

            bool change = false;
            CSV.NPE._1999? csvnpe1999 = row as CSV.NPE._1999;
            CSV.RowLGE? csvrowlge = row as CSV.RowLGE;
            CSV.RowNPE2? csvrownpe2 = row as CSV.RowNPE2;

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
}