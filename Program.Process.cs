using Database.IEC.Inputs.CSVs;

using SQLite;

using System;

using XycloneDesigns.Apis.IEC.Tables;

namespace Database.IEC
{
    internal partial class Program
    {
		public static void Process<TRow>(Parameters<TRow> parameters) where TRow : CSVRow
		{
			Console.WriteLine(parameters.LoggerTag);

			if (parameters.ElectoralEvent is null)
			{
				Console.WriteLine("{0} - No Electoral Event!", parameters.LoggerTag);
				return;
			}

			Console.WriteLine("{0} - New Municipalities", parameters.LoggerTag); parameters.Municipalities =
				parameters.SqliteConnectionMunicipalities?.CSVNewMunicipalities(parameters.Logger, parameters.Rows);

			Console.WriteLine("{0} - New Parties", parameters.LoggerTag); parameters.Parties = 
				parameters.SqliteConnection?.CSVNewParties(parameters.Logger, parameters.Rows);
			
			Console.WriteLine("{0} - New VotingDistricts", parameters.LoggerTag); parameters.VotingDistricts = 
				parameters.SqliteConnection?.CSVNewVotingDistricts(parameters.Logger, parameters.Rows);
			
			Console.WriteLine("{0} - New Wards", parameters.LoggerTag); parameters.Wards = 
				parameters.SqliteConnection?.CSVNewWards(parameters.Logger, parameters.Rows);
			
			Console.WriteLine("{0} - CSV", parameters.LoggerTag); 
			
			CSV(parameters);

			parameters.Rows.Clear();

			string pathdb = ElectoralEventPath(DirectoryOutput, parameters.ElectoralEvent, "db");
			string pathtxt = ElectoralEventPath(DirectoryOutput, parameters.ElectoralEvent, "txt");

			PksToText(pathtxt, parameters);

			if (parameters.SqliteConnection?
				.Table<Ballot>()
				.Where(ballot => ballot.PkElectoralEvent == parameters.ElectoralEvent.Pk) is TableQuery<Ballot> ballots)
			{
				using SQLiteConnection connectionNE1994 = SQLiteConnection(pathdb);

				connectionNE1994.InsertAll(ballots);
				connectionNE1994.Close();
			}
		}
	}
}