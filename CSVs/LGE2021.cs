
namespace DataProcessor.CSVs
{
    public class LGE2021 : CSVRowLGE3
    {
        public LGE2021(string line) : base(line)
        {
            ElectoralEvent = Tables.ElectoralEvent.Types.Municipal + "2021";
        }

        public static bool IsElectoralEvent(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsMunicipal(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2021, electoralEvent.date);
        }
    }
}