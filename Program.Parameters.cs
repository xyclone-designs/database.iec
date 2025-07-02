using Database.IEC.Inputs.CSVs;

using SQLite;

using System;
using System.Collections.Generic;
using System.IO;

using XycloneDesigns.Apis.IEC.Tables;
using XycloneDesigns.Apis.General.Tables;

namespace Database.IEC
{
    internal partial class Program
    {
		public class Parameters<TCSVRow> : Parameters where TCSVRow : CSVRow
		{
			public Parameters() { }
			public Parameters(Parameters parameters)
			{
				NewBallots = parameters.NewBallots;
				NewElectoralEvents = parameters.NewElectoralEvents;
				NewParties = parameters.NewParties;
				NewMunicipalities = parameters.NewMunicipalities;
				NewProvinces = parameters.NewProvinces;
				NewVotingDistricts = parameters.NewVotingDistricts;
				NewWards = parameters.NewWards;

				Ballots = parameters.Ballots;
				BallotsElectoralEvent = parameters.BallotsElectoralEvent;
				ElectoralEvents = parameters.ElectoralEvents;
				Parties = parameters.Parties;
				Municipalities = parameters.Municipalities;
				Provinces = parameters.Provinces;
				VotingDistricts = parameters.VotingDistricts;
				Wards = parameters.Wards;

				SqliteConnection = parameters.SqliteConnection;
				SqliteConnectionMunicipalities = parameters.SqliteConnectionMunicipalities;
				SqliteConnectionProvinces = parameters.SqliteConnectionProvinces;

				Logger = parameters.Logger;
				LoggerTag = parameters.LoggerTag;
			}

			public List<TCSVRow>? Rows1 { get; set; } 
			public List<TCSVRow>? Rows2 { get; set; } 
			public ElectoralEvent? ElectoralEvent1 { get; set; }
			public ElectoralEvent? ElectoralEvent2 { get; set; }

			public Action<Parameters<TCSVRow>>? ExtraAction { get; set; }
		}
		public class Parameters
		{
			public bool New 
			{
				get =>
					(NewBallots.HasValue && NewBallots.Value) ||
					(NewElectoralEvents.HasValue && NewElectoralEvents.Value) ||
					(NewParties.HasValue && NewParties.Value) ||
					(NewMunicipalities.HasValue && NewMunicipalities.Value) ||
					(NewProvinces.HasValue && NewProvinces.Value) ||
					(NewVotingDistricts.HasValue && NewVotingDistricts.Value) ||
					(NewWards.HasValue && NewWards.Value);
			}
			public bool? NewBallots { get; set; }
			public bool? NewElectoralEvents { get; set; }
			public bool? NewParties { get; set; }
			public bool? NewMunicipalities { get; set; }
			public bool? NewProvinces { get; set; }
			public bool? NewVotingDistricts { get; set; }
			public bool? NewWards { get; set; }

			public List<Ballot>? Ballots { get; set; }
			public List<Ballot>? BallotsElectoralEvent { get; set; }
			public List<ElectoralEvent>? ElectoralEvents { get; set; }
			public List<Party>? Parties { get; set; }
			public List<Municipality>? Municipalities { get; set; }
			public List<Province>? Provinces { get; set; }
			public List<VotingDistrict>? VotingDistricts { get; set; }
			public List<Ward>? Wards { get; set; }

			public SQLiteConnection? SqliteConnection { get; set; }
			public SQLiteConnection? SqliteConnectionMunicipalities { get; set; }
			public SQLiteConnection? SqliteConnectionProvinces { get; set; }

			public StreamWriter? Logger { get; set; }
			public string? LoggerTag { get; set; }
		}
	}
}