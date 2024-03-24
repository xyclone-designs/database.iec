
namespace DataProcessor.CSVs
{
    public class LGE2000 : CSVRowLGE1
    {
        public LGE2000(string line) : base(line) { }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsMunicipal(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2000, electoralEvent.date);
        }
    }
}