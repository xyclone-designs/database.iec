
namespace DataProcessor.CSVs
{
    public class NPE2004 : CSVRowNPE1
    {
        public NPE2004(string line) : base(line) { }

        public static bool IsElectoralEventNE(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsNational(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2004, electoralEvent.date);
        }
        public static bool IsElectoralEventPE(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsProvincial(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2004, electoralEvent.date);
        }
    }
}