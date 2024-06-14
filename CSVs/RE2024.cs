
namespace DataProcessor.CSVs
{
    public class RE2024 : CSVRowNPE4
    {
        public RE2024(string line) : base(line.Replace(",,", ","))
        {
            ElectoralEvent = Tables.ElectoralEvent.Types.Regional + "2024";
        }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsRegional(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2024, electoralEvent.date);
        }
    }
}