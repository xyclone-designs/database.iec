
namespace DataProcessor.CSVs
{
    public class NPE2009 : CSVRowNPE2
    {
        public NPE2009(string line) : base(line) { }

        public static bool IsElectoralEventNE(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsNational(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2009, electoralEvent.date);
        }
        public static bool IsElectoralEventPE(Tables.ElectoralEvent electoralEvent)
        {
            return
                Tables.ElectoralEvent.IsProvincial(electoralEvent.type) &&
                Tables.ElectoralEvent.IsYear(2009, electoralEvent.date);
        }
    }
}