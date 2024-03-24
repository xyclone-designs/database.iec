
namespace DataProcessor.CSVs
{
    public class PE2019 : CSVRowNPE3
    {
        public PE2019(string line) : base(line.Replace(",,", ","))
        {
            ElectoralEvent = Tables.ElectoralEvent.Types.Provincial + "2019";
        }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsProvincial(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2019, electoralEvent.date);
        }
    }
}