
namespace DataProcessor.CSVs
{
    public class PE2024 : CSVRowNPE4
    {
        public PE2024(string line) : base(line.Replace(",,", ","))
        {
            ElectoralEvent = Tables.ElectoralEvent.Types.Provincial + "2024";
        }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsProvincial(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2024, electoralEvent.date);
        }
    }
}