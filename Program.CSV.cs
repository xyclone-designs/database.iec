using Database.IEC.Inputs.CSVs;

using SQLite;

using System;
using System.Data;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using XycloneDesigns.Database.IEC.Tables;

namespace Database.IEC
{
    internal partial class Program
    {
		public class CSVParameters
		{
			public List<Ballot>? Ballots { get; set; }
			public List<Ballot>? BallotsElectoralEvent { get; set; }
			public List<Ballot>? BallotsIndividual { get; set; }
			public List<ElectoralEvent>? ElectoralEvents { get; set; }
			public List<ElectoralEvent>? ElectoralEventsIndividual { get; set; }
			public List<Party>? Parties { get; set; }
			public List<Party>? PartiesIndividual { get; set; }
			public List<Province>? Provinces { get; set; }
			public List<Province>? ProvincesIndividual { get; set; }
			public List<Municipality>? Municipalities { get; set; }
			public List<Municipality>? MunicipalitiesIndividual { get; set; }
			public List<VotingDistrict>? VotingDistricts { get; set; }
			public List<VotingDistrict>? VotingDistrictsIndividual { get; set; }
			public List<Ward>? Wards { get; set; }
			public List<Ward>? WardsIndividual { get; set; }
		}

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
                        else if (sqliteConnection.Table<Ward>().FirstOrDefault(_ => _.Id == wardid) is Ward wardsql)
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
                        else if (sqliteConnection.Table<Province>().FirstOrDefault(_ => _.Pk == rowsorderedenumerator.Current.ProvincePk) is Province Provincesql)
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
                        else if (sqliteConnection.Table<VotingDistrict>().FirstOrDefault(_ => _.Id == rowsorderedenumerator.Current.VotingDistrictId) is VotingDistrict votingdistrictsql)
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
							else if (sqliteConnection.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => _.Name == rowsorderedenumerator.Current.MunicipalityName) is Municipality municipalitysql)
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
                            else if (sqliteConnection.Table<Municipality>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.GeoCode, rowsorderedenumerator.Current.MunicipalityGeo, StringComparison.OrdinalIgnoreCase)) is Municipality municipalitysql)
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
                        currentWard.List_PkVotingDistrict = ElectionsItem.AddPKIfUnique(currentWard.List_PkVotingDistrict, currentVotingDistrict?.Pk);
                    }
                    if (currentMunicipality is not null)
                    {
                        currentMunicipality.PkProvince ??= rowsorderedenumerator.Current.ProvincePk;
                        currentMunicipality.List_PkWard = ElectionsItem.AddPKIfUnique(currentMunicipality.List_PkWard, currentWard?.Pk);
                    }

                    currentBallot = rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.PkElectoralEvent ??= electoralEvent.Pk;
                        ballot.PkProvince ??= currentProvince?.Pk ?? rowsorderedenumerator.Current.ProvincePk;
                        ballot.PkMunicipality ??= currentMunicipality?.Pk;
                        ballot.PkWard ??= currentWard?.Pk;
                        ballot.PkVotingDistrict ??= currentVotingDistrict?.Pk;
                    });
                    electoralEventBallot ??= rowsorderedenumerator.Current.AsBallot(ballot =>
                    {
                        ballot.PkElectoralEvent = electoralEvent.Pk;
                        ballot.PkProvince = null;
                        ballot.PkMunicipality = null;
                        ballot.PkWard = null;
                        ballot.PkVotingDistrict = null;
                        if (electoralEvent.Type?.Contains(ElectoralEvent.Types.Municipal) ?? false)
                            ballot.Type = ElectoralEvent.Types.Municipal;
                    });
                }

                if (rowsorderedenumerator.Current.PartyName is null)
                    currentParty = null;
                else if (currentParty is null || string.Equals(currentParty.Name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase) is false)
                {
                    if (parameters.Parties.Find(party => string.Equals(party.Name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase)) is Party party)
                        currentParty = party;
                    else if (sqliteConnection.Table<Party>().AsEnumerable().FirstOrDefault(_ => string.Equals(_.Name, rowsorderedenumerator.Current.PartyName, StringComparison.OrdinalIgnoreCase)) is Party partysql)
                    {
                        currentParty = partysql;
                        parameters.Parties.Add(partysql);
                    }
                }

                if (currentParty is not null && rowsorderedenumerator.Current.PartyVotes.HasValue)
                    currentBallot.List_PkParty_Votes = ElectionsItem.AddPKPairIfUnique(
                        currentBallot.List_PkParty_Votes, 
                        currentParty.Pk, 
                        rowsorderedenumerator.Current.PartyVotes.Value);
            }

            if (currentBallot?.PkElectoralEvent is not null)
                parameters.Ballots.Add(currentBallot);

            Console.WriteLine("Inserting Event Ballots");

            if (electoralEventBallot is not null)
            {
				if (ElectoralEvent.IsMunicipal(electoralEvent.Type))
					electoralEventBallot.VotersRegistered = null;

				electoralEventBallot = sqliteConnection.CreateAndAdd(electoralEventBallot);
                parameters.BallotsElectoralEvent.Add(electoralEventBallot);
                electoralEvent.List_PkBallot = ElectionsItem.AddPKIfUnique(electoralEvent.List_PkBallot, electoralEventBallot.Pk);
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
                Ballot added = sqliteConnection.CreateAndAdd(eventballot);
                parameters.BallotsElectoralEvent.Add(added);

                if (ElectoralEvent.IsMunicipal(electoralEvent.Type))
                {
                    if (electoralEventBallot is not null)
                        electoralEventBallot.VotersRegistered = Math.Max(
                            eventballot.VotersRegistered ?? 0,
                            electoralEventBallot.VotersRegistered ?? 0);

					if (added.PkMunicipality is null)
                        electoralEvent.List_PkBallot = ElectionsItem.AddPKIfUnique(electoralEvent.List_PkBallot, added.Pk);
                }
                else electoralEvent.List_PkBallot = ElectionsItem.AddPKIfUnique(electoralEvent.List_PkBallot, added.Pk);
            }

            sqliteConnection.Update(electoralEventBallot);

            Console.WriteLine("Inserting Ballots");
            if (electoralEvent.Pk != 1 && electoralEvent.Pk != 2)
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
                party.List_PkElectoralEvent = ElectionsItem.AddPKIfUnique(party.List_PkElectoralEvent, electoralEvent.Pk);
            sqliteConnection.UpdateAll(parameters.Parties);
            Console.WriteLine("Updating ElectoralEvent");
            sqliteConnection.Update(electoralEvent);

            parameters.ElectoralEvents?.Add(electoralEvent);
        }
    }
}