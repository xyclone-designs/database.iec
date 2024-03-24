
namespace DataProcessor.CSVs
{
    public class LGE2016 : CSVRowLGE3
    {
        public LGE2016(string line) : base(line) 
        {
            ElectoralEvent = Tables.ElectoralEvent.Types.Municipal + "2016";
        }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsMunicipal(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2016, electoralEvent.date);
        }
    }
}