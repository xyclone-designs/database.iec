
namespace DataProcessor.CSVs
{
    public class LGE2011 : CSVRowLGE2
    {
        public LGE2011(string line) : base(line) { }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsMunicipal(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2011, electoralEvent.date);
        }
    }
}