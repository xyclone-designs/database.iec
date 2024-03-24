
namespace DataProcessor.CSVs
{
    public class LGE2006 : CSVRowLGE2
    {
        public LGE2006(string line) : base(line) { }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsMunicipal(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2006, electoralEvent.date);
        }
    }
}