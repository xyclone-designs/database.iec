using DataProcessor.CSVs;
using DataProcessor.Tables;

using SQLite;

using System;
using System.Data;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;

namespace DataProcessor
{
    internal partial class Program
    {
        public static void CSV<TCSVRow>(SQLiteConnection sqliteConnection, StreamWriter log, ElectoralEvent electoralEvent, IEnumerable<TCSVRow> rows, CSVParameters parameters) where TCSVRow : CSVRow
        {
            parameters.Ballots = [];
            parameters.BallotsElectoralEvent = [];
            parameters.Parties ??= [];
            parameters.Provinces ??= [];
            parameters.VotingDistricts ??= [];
            parameters.Wards ??= [];
            parameters.Municipalities ??= [];

            Ballot? electoralEventBallot = null;
            Ballot? currentBallot = null;
            VotingDistrict? currentVotingDistrict = null;
            Province? currentProvince = null;
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
                    {
                        parameters.Ballots.Add(currentBallot);
                        electoralEventBallot?.UpdateBallot(currentBallot);
                    }

                    if (rowsorderedenumerator.Current.GetWardId() is not string wardid)
                        currentWard = null;
                    else if (currentWard is null || currentWard.id != wardid)
                    {
                        if (parameters.Wards.Find(ward => ward.id == wardid) is Ward ward)
                            currentWard = ward;
                        else if (sqliteConnection.Table<Ward>().FirstOrDefault(_ => _.id == wardid) is Ward wardsql)
                        {
                            currentWard = wardsql;
                            parameters.Wards.Add(currentWard);
                        }
                    }

                    if (rowsorderedenumerator.Current.ProvincePk is null)
                        currentProvince = null;
                    else if (currentProvince is null || currentProvince.pk != rowsorderedenumerator.Current.ProvincePk)
                    {
                        if (parameters.Provinces.Find(currentProvince => currentProvince.pk == rowsorderedenumerator.Current.ProvincePk) is Province Province)
                            currentProvince = Province;
                        else if (sqliteConnection.Table<Province>().FirstOrDefault(_ => _.pk == rowsorderedenumerator.Current.ProvincePk) is Province Provincesql)
                        {
                            currentProvince = Provincesql;
                            parameters.Provinces.Add(currentProvince);
                        }
                    }

                    if (rowsorderedenumerator.Current.VotingDistrictId is null)
                        currentVotingDistrict = null;
                    else if (currentVotingDistrict is null || currentVotingDistrict.id != rowsorderedenumerator.Current.VotingDistrictId)
                    {
                        if (parameters.VotingDistricts.Find(votingdistrict => votingdistrict.id == rowsorderedenumerator.Current.VotingDistrictId) is VotingDistrict votingdistrict)
                            currentVotingDistrict = votingdistrict;
                        else if (sqliteConnection.Table<VotingDistrict>().FirstOrDefault(_ => _.id == rowsorderedenumerator.Current.VotingDistrictId) is VotingDistrict votingdistrictsql)
                        {
                            currentVotingDistrict = votingdistrictsql;
                            parameters.VotingDistricts.Add(currentVotingDistrict);
                        }
                    }

                    if (typeof(TCSVRow) == typeof(NPE1999))
                    {
                        if (rowsorderedenumerator.Current.MunicipalityName is null)
                            currentMunicipality = null;
						else if (currentMunicipality is null || string.Equals(currentMunicipality.name, rowsorderedenumerator.Current.MunicipalityName, StringComparison.OrdinalIgnoreCase) is false)
						{
							if (parameters.Municipalities.Find(_ => string.Equals(_.name, rowsorderedenumerator.Current.MunicipalityName, StringComparison.OrdinalIgnoreCase)) is Municipality municipality)
								currentMunicipality = municipality;
							else if (sqliteConnection.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => _.name == rowsorderedenumerator.Current.MunicipalityName) is Municipality municipalitysql)
                            {
                                currentMunicipality = municipalitysql;
                                parameters.Municipalities.Add(municipalitysql);
                            }
                        }
                    }
                    else
                    {
                        if (rowsorderedenumerator.Current.MunicipalityGeo is null)
                            currentMunicipality = null;
                        else if (currentMunicipality is null || string.Equals(currentMunicipality.geoCode, rowsorderedenumerator.Current.MunicipalityGeo, StringComparison.OrdinalIgnoreCase) is false)
                        {
                            if (parameters.Municipalities.Find(_ => string.Equals(_.geoCode, rowsorderedenumerator.Current.MunicipalityGeo, StringComparison.OrdinalIgnoreCase)) is Municipality municipality)
                                currentMunicipality = municipality;
                            else if (sqliteConnection.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.geoCode, rowsorderedenumerator.Current.MunicipalityGeo, StringComparison.OrdinalIgnoreCase)) is Municipality municipalitysql)
                            {
                                currentMunicipality = municipalitysql;
                                parameters.Municipalities.Add(municipalitysql);
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
                        currentWard.list_pkVotingDistrict = ElectionsItem.AddPKIfUnique(currentWard.list_pkVotingDistrict, currentVotingDistrict?.pk);
                    }
                    if (currentMunicipality is not null)
                    {
                        currentMunicipality.pkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                        currentMunicipality.list_pkWard = ElectionsItem.AddPKIfUnique(currentMunicipality.list_pkWard, currentWard?.pk);
                    }

                    currentBallot = rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.pkElectoralEvent ??= electoralEvent.pk;
                        ballot.pkProvince ??= currentProvince?.pk ?? rowsorderedenumerator.Current.ProvincePk;
                        ballot.pkMunicipality ??= currentMunicipality?.pk;
                        ballot.pkWard ??= currentWard?.pk;
                        ballot.pkVotingDistrict ??= currentVotingDistrict?.pk;
                    });
                    electoralEventBallot ??= rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.pkElectoralEvent = electoralEvent.pk;
                        ballot.pkProvince = null;
                        ballot.pkMunicipality = null;
                        ballot.pkWard = null;
                        ballot.pkVotingDistrict = null;
                        if (electoralEvent.type?.Contains(ElectoralEvent.Types.Municipal) ?? false)
                            ballot.type = ElectoralEvent.Types.Municipal;
                    });
                }

                if (rowsorderedenumerator.Current.PartyName is null)
                    currentParty = null;
                else if (currentParty is null || string.Equals(currentParty.name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase) is false)
                {
                    if (parameters.Parties.Find(party => string.Equals(party.name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase)) is Party party)
                        currentParty = party;
                    else if (sqliteConnection.Table<Party>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase)) is Party partysql)
                    {
                        currentParty = partysql;
                        parameters.Parties.Add(partysql);
                    }
                }

                if (currentParty is not null && rowsorderedenumerator.Current.PartyVotes.HasValue)
                    currentBallot.list_pkParty_votes = ElectionsItem.AddPKPairIfUnique(
                        currentBallot.list_pkParty_votes, 
                        currentParty.pk, 
                        rowsorderedenumerator.Current.PartyVotes.Value);
            }

            if (currentBallot?.pkElectoralEvent is not null)
                parameters.Ballots.Add(currentBallot);

            Console.WriteLine("Inserting Event Ballots");

            if (electoralEventBallot is not null)
            {
				if (ElectoralEvent.IsMunicipal(electoralEvent.type))
					electoralEventBallot.votersRegistered = null;

				electoralEventBallot = sqliteConnection.CreateAndAdd(electoralEventBallot);
                parameters.BallotsElectoralEvent.Add(electoralEventBallot);
                electoralEvent.list_pkBallot = ElectionsItem.AddPKIfUnique(electoralEvent.list_pkBallot, electoralEventBallot.pk);
            }

            foreach (Ballot eventballot in parameters.Ballots
                .Where(_ballot => string.IsNullOrWhiteSpace(_ballot.type) is false && _ballot.pkProvince.HasValue)
                .GroupBy(_ballot =>
                {
                    return
                        _ballot.type!.Split('.') is string[] typesplit &&
                        typesplit.Length == 2 &&
                        typesplit[0] == ElectoralEvent.Types.Municipal
                            ? _ballot.type
                            : string.Format("{0}.{1}", _ballot.type, CSVRow.Utils.ProvincePkToId(_ballot.pkProvince!.Value));
                })
                .Where(groupedballot => groupedballot.Any())
                .SelectMany(groupedballot =>
                {
                    IEnumerable<Ballot> ballots = Enumerable.Empty<Ballot>();
                    bool ismunicipal = groupedballot.Key.Contains(ElectoralEvent.Types.Municipal, StringComparison.OrdinalIgnoreCase);
                    Ballot _eventballot = new() { type = groupedballot.Key, };
                    Ballot? _municipalityballot = null;

                    foreach (Ballot ballot in groupedballot.OrderBy(_ => _.pkProvince).ThenBy(_ => _.pkMunicipality))
                    {
                        if (ismunicipal)
                        {
                            if (_municipalityballot is null ||
                                (ballot.pkMunicipality is not null && _municipalityballot.pkMunicipality is null) ||
                                (ballot.pkMunicipality is null && _municipalityballot.pkMunicipality is not null) ||
                                (
                                    ballot.pkMunicipality is not null &&
                                    _municipalityballot.pkMunicipality is not null &&
                                    _municipalityballot.pkMunicipality.Value != ballot.pkMunicipality.Value

                                ))
                            {
                                if (_municipalityballot is not null)
                                    ballots = ballots.Append(_municipalityballot);

                                _municipalityballot = new() { type = groupedballot.Key, };
                            }

                            _municipalityballot.pkProvince ??= ballot.pkProvince;
                            _municipalityballot.pkMunicipality ??= ballot.pkMunicipality;
                            _municipalityballot.pkElectoralEvent ??= ballot.pkElectoralEvent;
                            _municipalityballot.UpdateBallot(ballot);
                        }

                        _eventballot.UpdateBallot(ballot);
                        _eventballot.pkProvince ??= ismunicipal ? null : ballot.pkProvince;
                        _eventballot.pkElectoralEvent ??= ballot.pkElectoralEvent;
                    }

                    electoralEventBallot?.UpdatePartyVotes(_eventballot.list_pkParty_votes);

                    return ballots.Prepend(_eventballot);

                }).OrderBy(_ => _.pkMunicipality.HasValue))
            {
                Ballot added = sqliteConnection.CreateAndAdd(eventballot);
                parameters.BallotsElectoralEvent.Add(added);

                if (ElectoralEvent.IsMunicipal(electoralEvent.type))
                {
                    if (electoralEventBallot is not null)
                        electoralEventBallot.votersRegistered = Math.Max(
                            eventballot.votersRegistered ?? 0,
                            electoralEventBallot.votersRegistered ?? 0);

					if (added.pkMunicipality is null)
                        electoralEvent.list_pkBallot = ElectionsItem.AddPKIfUnique(electoralEvent.list_pkBallot, added.pk);
                }
                else electoralEvent.list_pkBallot = ElectionsItem.AddPKIfUnique(electoralEvent.list_pkBallot, added.pk);
            }

            sqliteConnection.Update(electoralEventBallot);

            Console.WriteLine("Inserting Ballots");
            if (electoralEvent.pk != 1 && electoralEvent.pk != 2)
                sqliteConnection.InsertAll(parameters.Ballots);
            Console.WriteLine("Updating Provinces");
            sqliteConnection.UpdateAll(parameters.Provinces);
            Console.WriteLine("Updating Municipalities");
            sqliteConnection.UpdateAll(parameters.Municipalities);
            Console.WriteLine("Updating VotingDistricts");
            sqliteConnection.UpdateAll(parameters.VotingDistricts);
            Console.WriteLine("Updating Wards");
            sqliteConnection.UpdateAll(parameters.Wards);
            Console.WriteLine("Updating Parties");
            foreach (Party party in parameters.Parties)
                party.list_pkElectoralEvent = ElectionsItem.AddPKIfUnique(party.list_pkElectoralEvent, electoralEvent.pk);
            sqliteConnection.UpdateAll(parameters.Parties);
            Console.WriteLine("Updating ElectoralEvent");
            sqliteConnection.Update(electoralEvent);

            parameters.ElectoralEvents?.Add(electoralEvent);
        }
    }
}