using Database.IEC.Inputs.CSVs;

using SQLite;

using System;
using System.Data;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using XycloneDesigns.Apis.IEC.Tables;
using XycloneDesigns.Apis.General.Tables;

using _TableGeneral = XycloneDesigns.Apis.General.Tables._Table;

namespace Database.IEC
{
    internal partial class Program
    {
		public static void CSV<TCSVRow>(Parameters<TCSVRow> parameters) where TCSVRow : CSVRow
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

            IOrderedEnumerable<CSVRow> rowsordered = parameters.Rows.OrderBy(row => row.ProvincePk);
            rowsordered = typeof(TCSVRow) == typeof(NPE1999)
                ? rowsordered.ThenBy(row => row.MunicipalityName)
                : rowsordered.ThenBy(row => row.MunicipalityGeo);
            rowsordered = rowsordered.ThenBy(row => row.GetWardId());
            rowsordered = rowsordered.ThenBy(row => row.VotingDistrictId);
            rowsordered = rowsordered.ThenBy(row => row.GetBallotType());

            IEnumerator<CSVRow> rowsorderedenumerator = rowsordered.GetEnumerator();

            for (int index = 0; rowsorderedenumerator.MoveNext(); index++)
            {
                Console.WriteLine("Row {0} / {1}", index, parameters.Rows.Count);

                if (currentBallot is null || ChangeBallot(
                    rowsorderedenumerator.Current,
                    currentBallot,
                    currentVotingDistrict?.Id,
                    currentWard?.Id,
                    typeof(TCSVRow) == typeof(NPE1999) ? currentMunicipality?.Name : currentMunicipality?.GeoCode))
                {
                    if (currentBallot?.PkElectoralEvent is not null)
                    {
                        parameters.Ballots.Add(currentBallot);
                        electoralEventBallot?.UpdateBallot(currentBallot);
                    }

                    if (rowsorderedenumerator.Current.GetWardId() is not string wardid)
                        currentWard = null;
                    else if (currentWard is null || currentWard.Id != wardid)
                    {
                        if (parameters.Wards.Find(ward => ward.Id == wardid) is Ward ward)
                            currentWard = ward;
                        else if (parameters.SqliteConnection?.Table<Ward>().FirstOrDefault(_ => _.Id == wardid) is Ward wardsql)
                        {
                            currentWard = wardsql;
                            parameters.Wards.Add(currentWard);
                        }
                    }

                    if (rowsorderedenumerator.Current.ProvincePk is null)
                        currentProvince = null;
                    else if (currentProvince is null || currentProvince.Pk != rowsorderedenumerator.Current.ProvincePk)
                    {
                        if (parameters.Provinces.Find(currentProvince => currentProvince.Pk == rowsorderedenumerator.Current.ProvincePk) is Province Province)
                            currentProvince = Province;
                        else if (parameters.SqliteConnection?.Table<Province>().FirstOrDefault(_ => _.Pk == rowsorderedenumerator.Current.ProvincePk) is Province Provincesql)
                        {
                            currentProvince = Provincesql;
                            parameters.Provinces.Add(currentProvince);
                        }
                    }

                    if (rowsorderedenumerator.Current.VotingDistrictId is null)
                        currentVotingDistrict = null;
                    else if (currentVotingDistrict is null || currentVotingDistrict.Id != rowsorderedenumerator.Current.VotingDistrictId)
                    {
                        if (parameters.VotingDistricts.Find(votingdistrict => votingdistrict.Id == rowsorderedenumerator.Current.VotingDistrictId) is VotingDistrict votingdistrict)
                            currentVotingDistrict = votingdistrict;
                        else if (parameters.SqliteConnection?.Table<VotingDistrict>().FirstOrDefault(_ => _.Id == rowsorderedenumerator.Current.VotingDistrictId) is VotingDistrict votingdistrictsql)
                        {
                            currentVotingDistrict = votingdistrictsql;
                            parameters.VotingDistricts.Add(currentVotingDistrict);
                        }
                    }

                    if (typeof(TCSVRow) == typeof(NPE1999))
                    {
                        if (rowsorderedenumerator.Current.MunicipalityName is null)
                            currentMunicipality = null;
						else if (currentMunicipality is null || string.Equals(currentMunicipality.Name, rowsorderedenumerator.Current.MunicipalityName, StringComparison.OrdinalIgnoreCase) is false)
						{
							if (parameters.Municipalities.Find(_ => string.Equals(_.Name, rowsorderedenumerator.Current.MunicipalityName, StringComparison.OrdinalIgnoreCase)) is Municipality municipality)
								currentMunicipality = municipality;
							else if (parameters.SqliteConnectionMunicipalities?.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => _.Name == rowsorderedenumerator.Current.MunicipalityName) is Municipality municipalitysql)
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
                        else if (currentMunicipality is null || string.Equals(currentMunicipality.GeoCode, rowsorderedenumerator.Current.MunicipalityGeo, StringComparison.OrdinalIgnoreCase) is false)
                        {
                            if (parameters.Municipalities.Find(_ => string.Equals(_.GeoCode, rowsorderedenumerator.Current.MunicipalityGeo, StringComparison.OrdinalIgnoreCase)) is Municipality municipality)
                                currentMunicipality = municipality;
                            else if (parameters.SqliteConnectionMunicipalities?.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.GeoCode, rowsorderedenumerator.Current.MunicipalityGeo, StringComparison.OrdinalIgnoreCase)) is Municipality municipalitysql)
                            {
                                currentMunicipality = municipalitysql;
                                parameters.Municipalities.Add(municipalitysql);
                            }
                        }
                    }
                    if (currentVotingDistrict is not null)
                    {
                        currentVotingDistrict.PkWard ??= currentWard?.Pk;
                        currentVotingDistrict.PkMunicipality ??= currentMunicipality?.Pk;
                        currentVotingDistrict.PkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                    }
                    if (currentWard is not null)
                    {
                        currentWard.PkMunicipality ??= currentMunicipality?.Pk;
                        currentWard.PkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                    }
                    if (currentMunicipality is not null)
                    {
                        currentMunicipality.PkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                    }

                    currentBallot = rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.PkElectoralEvent ??= parameters.ElectoralEvent?.Pk;
                        ballot.PkProvince ??= currentProvince?.Pk ?? rowsorderedenumerator.Current.ProvincePk;
                        ballot.PkMunicipality ??= currentMunicipality?.Pk;
                        ballot.PkWard ??= currentWard?.Pk;
                        ballot.PkVotingDistrict ??= currentVotingDistrict?.Pk;
                    });
                    electoralEventBallot ??= rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.PkElectoralEvent = parameters.ElectoralEvent?.Pk;
                        ballot.PkProvince = null;
                        ballot.PkMunicipality = null;
                        ballot.PkWard = null;
                        ballot.PkVotingDistrict = null;
                        if (parameters.ElectoralEvent?.Type?.Contains(ElectoralEvent.Types.Municipal) ?? false)
                            ballot.Type = ElectoralEvent.Types.Municipal;
                    });
                }

                if (rowsorderedenumerator.Current.PartyName is null)
                    currentParty = null;
                else if (currentParty is null || string.Equals(currentParty.Name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase) is false)
                {
                    if (parameters.Parties.Find(party => string.Equals(party.Name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase)) is Party party)
                        currentParty = party;
                    else if (parameters.SqliteConnection?.Table<Party>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.Name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase)) is Party partysql)
                    {
                        currentParty = partysql;
                        parameters.Parties.Add(partysql);
                    }
                }

                if (currentParty is not null && rowsorderedenumerator.Current.PartyVotes.HasValue)
                    currentBallot.List_PkParty_Votes = _TableGeneral.AddPKPairIfUnique(
                        currentBallot.List_PkParty_Votes, 
                        currentParty.Pk, 
                        rowsorderedenumerator.Current.PartyVotes.Value);
            }

            if (currentBallot?.PkElectoralEvent is not null)
                parameters.Ballots.Add(currentBallot);

            Console.WriteLine("Inserting Event Ballots");

            if (electoralEventBallot is not null)
            {
				if (ElectoralEvent.IsMunicipal(parameters.ElectoralEvent?.Type))
					electoralEventBallot.VotersRegistered = null;

                if (parameters.SqliteConnection is not null)
                {
					electoralEventBallot = parameters.SqliteConnection.CreateAndAdd(electoralEventBallot);

					parameters.BallotsElectoralEvent.Add(electoralEventBallot);
					if (parameters.ElectoralEvent is not null)
						parameters.ElectoralEvent.List_PkBallot = _TableGeneral.AddPKIfUnique(parameters.ElectoralEvent.List_PkBallot, electoralEventBallot.Pk);
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
                            : string.Format("{0}.{1}", _ballot.Type, CSVRow.Utils.ProvincePkToId(_ballot.PkProvince!.Value));
                })
                .Where(groupedballot => groupedballot.Any())
                .SelectMany(groupedballot =>
                {
                    IEnumerable<Ballot> ballots = Enumerable.Empty<Ballot>();
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

                    electoralEventBallot?.UpdatePartyVotes(_eventballot.List_PkParty_Votes);

                    return ballots.Prepend(_eventballot);

                }).OrderBy(_ => _.PkMunicipality.HasValue))
            {
                Ballot? added = parameters.SqliteConnection?.CreateAndAdd(eventballot);
                
                if (added is not null)
                    parameters.BallotsElectoralEvent.Add(added);

                if (parameters.ElectoralEvent is not null)
                {
					if (ElectoralEvent.IsMunicipal(parameters.ElectoralEvent.Type))
					{
						if (electoralEventBallot is not null)
							electoralEventBallot.VotersRegistered = Math.Max(
								eventballot.VotersRegistered ?? 0,
								electoralEventBallot.VotersRegistered ?? 0);

						if (added?.PkMunicipality is null)
							parameters.ElectoralEvent.List_PkBallot = _TableGeneral.AddPKIfUnique(parameters.ElectoralEvent.List_PkBallot, added?.Pk);
					}
					else parameters.ElectoralEvent.List_PkBallot = _TableGeneral.AddPKIfUnique(parameters.ElectoralEvent.List_PkBallot, added?.Pk);
				}
            }

			parameters.SqliteConnection?.Update(electoralEventBallot);

            if (parameters.SqliteConnection is not null)
            {
				Console.WriteLine("Updating ElectoralEvent"); parameters.SqliteConnection.Update(parameters.ElectoralEvent);
				Console.WriteLine("Updating Provinces"); parameters.SqliteConnection.UpdateAll(parameters.Provinces);
				Console.WriteLine("Updating VotingDistricts"); parameters.SqliteConnection.UpdateAll(parameters.VotingDistricts);
				Console.WriteLine("Updating Wards"); parameters.SqliteConnection.UpdateAll(parameters.Wards);

				Console.WriteLine("Inserting Ballots");
				if (parameters.ElectoralEvent?.Pk != 1 && parameters.ElectoralEvent?.Pk != 2)
					parameters.SqliteConnection.InsertAll(parameters.Ballots);

				Console.WriteLine("Updating Parties");
                foreach (Party party in parameters.Parties)
					party.List_PkElectoralEvent = _TableGeneral.AddPKIfUnique(party.List_PkElectoralEvent, parameters.ElectoralEvent?.Pk);
				parameters.SqliteConnection.UpdateAll(parameters.Parties);
			}

            if (parameters.SqliteConnectionMunicipalities is not null)
			{
				Console.WriteLine("Updating Municipalities"); parameters.SqliteConnectionMunicipalities.UpdateAll(parameters.Municipalities);
			}
        
            if (parameters.ElectoralEvent is not null)
                parameters.ElectoralEvents?.Add(parameters.ElectoralEvent);
        }
    }
}