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
        public static void XLS<TXLSSeats, TXLSSeatsRow>(SQLiteConnection sqliteConnection, ElectoralEvent electoralEvent, IEnumerable<TXLSSeats> seats) 
            where TXLSSeatsRow : XLSs.XLSSeatsRow 
            where TXLSSeats : XLSs.XLSSeats<TXLSSeatsRow>
		{
			Ballot ballotmunicipalitypr = sqliteConnection.Table<Ballot>()
				.First(ballot =>
					ballot.type != null &&
					ballot.type.Contains("pr") &&
					ballot.pkElectoralEvent == electoralEvent.pk &&
					ballot.pkProvince == null &&
					ballot.pkMunicipality == null &&
					ballot.pkVotingDistrict == null &&
					ballot.pkWard == null);
			Ballot ballotmunicipalityward = sqliteConnection.Table<Ballot>()
				.First(ballot =>
					ballot.type != null &&
					ballot.type.Contains("ward") &&
					ballot.pkElectoralEvent == electoralEvent.pk &&
					ballot.pkProvince == null &&
					ballot.pkMunicipality == null &&
					ballot.pkVotingDistrict == null &&
					ballot.pkWard == null);
			List<Ballot> ballotsmunicipality = sqliteConnection.Table<Ballot>()
				.Where(ballot =>
					ballot.type != null &&
					(ballot.type.Contains("pr") || ballot.type.Contains("ward")) &&
					ballot.pkElectoralEvent == electoralEvent.pk &&
					ballot.pkProvince != null &&
					ballot.pkMunicipality != null &&
					ballot.pkVotingDistrict == null &&
					ballot.pkWard == null)
                .ToList();

            ballotmunicipalitypr.list_pkParty_seats = null;
			ballotmunicipalityward.list_pkParty_seats = null;
            foreach (Ballot ballotmunicipality in ballotsmunicipality)
				ballotmunicipality.list_pkParty_seats = null;

			foreach (Ballot ballotmunicipality in ballotsmunicipality)
			{
				Municipality municipality = sqliteConnection.Find<Municipality>(ballotmunicipality.pkMunicipality);
				TXLSSeats? txlsseats = seats.FirstOrDefault(_seat => _seat.MunicipalityGeo == municipality?.geoCode);

                if (txlsseats?.Rows != null)
                    foreach (TXLSSeatsRow txlsseatsrow in txlsseats.Rows)
                    {
                        bool 
                            isPr = ballotmunicipality.type!.Contains("pr"),
                            isWard = ballotmunicipality.type!.Contains("ward");

                        (int? PartySeats, string? PartyName) = true switch
						{
							true when txlsseatsrow is XLSs.XLSSeatsLGE1Row xlsseatslge1row && isPr => ((int?)xlsseatslge1row.PRListSeats, xlsseatslge1row.PartyName),
                            true when txlsseatsrow is XLSs.XLSSeatsLGE1Row xlsseatslge1row && isWard => ((int?)xlsseatslge1row.WardSeats, xlsseatslge1row.PartyName),
                            true when txlsseatsrow is XLSs.XLSSeatsLGE2Row xlsseatslge2row => ((int?)xlsseatslge2row.TotalPartySeats, xlsseatslge2row.PartyName),
                            true when txlsseatsrow is XLSs.XLSSeatsLGE3Row xlsseatslge3row => ((int?)xlsseatslge3row.TotalPartySeats, xlsseatslge3row.PartyName),

                            _ => (new int?(), null)
                        };

						if (PartySeats.HasValue)
						{
                            Party party = sqliteConnection.Table<Party>().First(_party => _party.name == PartyName);

							ballotmunicipality.list_pkParty_seats = ElectionsItem.AddPKPairIfUnique(ballotmunicipality.list_pkParty_seats, party.pk, PartySeats.Value, true);
							if (ballotmunicipalitypr is not null && ballotmunicipality.type!.Contains("pr"))
								ballotmunicipalitypr.list_pkParty_seats = ElectionsItem.AddPKPairIfUnique(ballotmunicipalitypr.list_pkParty_seats, party.pk, PartySeats.Value, true);
							if (ballotmunicipalityward is not null && ballotmunicipality.type!.Contains("ward"))
								ballotmunicipalityward.list_pkParty_seats = ElectionsItem.AddPKPairIfUnique(ballotmunicipalityward.list_pkParty_seats, party.pk, PartySeats.Value, true);
						}
					}
			}

			sqliteConnection.Update(ballotmunicipalitypr, typeof(Ballot));
			sqliteConnection.Update(ballotmunicipalityward, typeof(Ballot));
			sqliteConnection.UpdateAll(ballotsmunicipality);
		}
    }
}