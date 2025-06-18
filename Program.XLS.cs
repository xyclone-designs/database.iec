using SQLite;

using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;

using XycloneDesigns.Apis.IEC.Tables;
using XycloneDesigns.Apis.General.Tables;

using _TableGeneral = XycloneDesigns.Apis.General.Tables._Table;

namespace Database.IEC
{
    internal partial class Program
    {
        public static void XLS<TXLSSeats, TXLSSeatsRow>(SQLiteConnection sqliteConnection, ElectoralEvent electoralEvent, IEnumerable<TXLSSeats> seats) 
            where TXLSSeatsRow : Inputs.XLSs.XLSSeatsRow 
            where TXLSSeats : Inputs.XLSs.XLSSeats<TXLSSeatsRow>
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
							true when txlsseatsrow is Inputs.XLSs.XLSSeatsLGE1Row xlsseatslge1row && isPr => ((int?)xlsseatslge1row.PRListSeats, xlsseatslge1row.PartyName),
                            true when txlsseatsrow is Inputs.XLSs.XLSSeatsLGE1Row xlsseatslge1row && isWard => ((int?)xlsseatslge1row.WardSeats, xlsseatslge1row.PartyName),
                            true when txlsseatsrow is Inputs.XLSs.XLSSeatsLGE2Row xlsseatslge2row => ((int?)xlsseatslge2row.TotalPartySeats, xlsseatslge2row.PartyName),
                            true when txlsseatsrow is Inputs.XLSs.XLSSeatsLGE3Row xlsseatslge3row => ((int?)xlsseatslge3row.TotalPartySeats, xlsseatslge3row.PartyName),

                            _ => (new int?(), null)
                        };

						if (PartySeats.HasValue)
						{
                            Party party = sqliteConnection.Table<Party>().AsEnumerable().First(_party => string.Equals(_party.Name, PartyName, StringComparison.OrdinalIgnoreCase));

							ballotmunicipality.List_PkParty_Seats = _TableGeneral.AddPKPairIfUnique(ballotmunicipality.List_PkParty_Seats, party.Pk, PartySeats.Value, true);
							if (ballotmunicipalitypr is not null && ballotmunicipality.Type!.Contains("pr"))
								ballotmunicipalitypr.List_PkParty_Seats = _TableGeneral.AddPKPairIfUnique(ballotmunicipalitypr.List_PkParty_Seats, party.Pk, PartySeats.Value, true);
							if (ballotmunicipalityward is not null && ballotmunicipality.Type!.Contains("ward"))
								ballotmunicipalityward.List_PkParty_Seats = _TableGeneral.AddPKPairIfUnique(ballotmunicipalityward.List_PkParty_Seats, party.Pk, PartySeats.Value, true);
						}
					}
			}

			sqliteConnection.Update(ballotmunicipalitypr, typeof(Ballot));
			sqliteConnection.Update(ballotmunicipalityward, typeof(Ballot));
			sqliteConnection.UpdateAll(ballotsmunicipality);
		}
    }
}