using SQLite;

using System;
using System.Collections.Generic;
using System.Linq;

using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC
{
    internal partial class Program
    {
		public static void Process<TCSVRow>(Parameters<TCSVRow> parameters) where TCSVRow : Inputs.CSVs.CSVRow
		{
			Console.WriteLine(parameters.LoggerTag);

			IEnumerable<TCSVRow> rows = Enumerable.Empty<TCSVRow>()
				.Concat(parameters.Rows1 ?? Enumerable.Empty<TCSVRow>())
				.Concat(parameters.Rows2 ?? Enumerable.Empty<TCSVRow>());

			Console.WriteLine("{0} - New Municipalities", parameters.LoggerTag); parameters.Municipalities =
				parameters.SqliteConnectionMunicipalities?.CSVNewMunicipalities(parameters.Logger, rows);

			Console.WriteLine("{0} - New Parties", parameters.LoggerTag); parameters.Parties = 
				parameters.SqliteConnection?.CSVNewParties(parameters.Logger, rows);
			
			Console.WriteLine("{0} - New VotingDistricts", parameters.LoggerTag); parameters.VotingDistricts = 
				parameters.SqliteConnection?.CSVNewVotingDistricts(parameters.Logger, rows);
			
			Console.WriteLine("{0} - New Wards", parameters.LoggerTag); parameters.Wards = 
				parameters.SqliteConnection?.CSVNewWards(parameters.Logger, rows);
			
			Console.WriteLine("{0} - Inputs.CSVs.CSV", parameters.LoggerTag);

			if (parameters.ElectoralEvent1 is not null && parameters.Rows1 is not null)
			{
				CSV(new Parameters<TCSVRow>(parameters) { ElectoralEvent1 = parameters.ElectoralEvent1, Rows1 = parameters.Rows1, });
				
				parameters.Rows1.Clear();				
			}
			if (parameters.ElectoralEvent2 is not null && parameters.Rows2 is not null)
			{
				CSV(new Parameters<TCSVRow>(parameters) { ElectoralEvent1 = parameters.ElectoralEvent2, Rows1 = parameters.Rows2, });

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
	}
}