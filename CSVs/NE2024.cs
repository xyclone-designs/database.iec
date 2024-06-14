
namespace DataProcessor.CSVs
{
    public class NE2024 : CSVRowNPE4
    {
        public NE2024(string line) : base(line.Replace(",,", ","))
        {
            ElectoralEvent = Tables.ElectoralEvent.Types.National + "2024";
        }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsNational(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2024, electoralEvent.date);
        }
    }
}