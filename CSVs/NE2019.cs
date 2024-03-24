
namespace DataProcessor.CSVs
{
    public class NE2019 : CSVRowNPE3
    {
        public NE2019(string line) : base(line)
        {
            ElectoralEvent = Tables.ElectoralEvent.Types.National + "2019";
        }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsNational(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2019, electoralEvent.date);
        }
    }
}